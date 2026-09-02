"""Live integration tests against a real Postgres/TimescaleDB instance.

Skipped unless FORECAST_STORE_TEST_DSN is set. Provisions the default store
(idempotent), exercises the resolvers and the write -> as-of read round trip,
and cleans up its own smoke data (the provisioned schema is left in place).
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

pytestmark = pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")

T0 = datetime(2024, 7, 30, tzinfo=timezone.utc)
SIM_AVAILABLE = datetime(2024, 7, 29, 6, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def conn():
    import psycopg

    from forecast_store.config import StoreConfig
    from forecast_store.provision import provision

    provision(DSN, StoreConfig())
    with psycopg.connect(DSN) as conn:
        _cleanup(conn)
        yield conn
        _cleanup(conn)


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        cur.execute(
            "DELETE FROM forecast.forecasts WHERE run_id IN "
            "(SELECT run_id FROM forecast.runs WHERE run_name = 'smoke')"
        )
        cur.execute("DELETE FROM forecast.runs WHERE run_name = 'smoke'")
        cur.execute("DELETE FROM forecast.series WHERE name = 'smoke_test'")
    conn.commit()


def test_reprovision_is_noop_and_drift_is_rejected():
    from forecast_store.config import StoreConfig
    from forecast_store.provision import MigrationRequired, provision

    report = provision(DSN, StoreConfig())
    assert report.already_provisioned
    with pytest.raises(MigrationRequired):
        provision(DSN, StoreConfig.from_levels(["0.1", "0.5", "0.9"]))


def test_resolvers(conn):
    import psycopg

    with conn.cursor() as cur:
        cur.execute(
            "SELECT forecast.register_series('smoke_test', interval '15 minutes')"
        )
        sid = cur.fetchone()[0]
        cur.execute(
            "SELECT forecast.register_series('smoke_test', interval '15 minutes')"
        )
        assert cur.fetchone()[0] == sid  # get-or-create is idempotent
        cur.execute("SELECT forecast.get_series_id('smoke_test')")
        assert cur.fetchone()[0] == sid
    conn.commit()

    with pytest.raises(psycopg.errors.RaiseException, match="unknown series name"):
        with conn.cursor() as cur:
            cur.execute("SELECT forecast.get_series_id('smoke_test_typo')")
    conn.rollback()


def test_write_and_asof_round_trip(conn):
    from forecast_store.config import StoreConfig
    from forecast_store.queries import forecast_asof

    config = StoreConfig()
    with conn.cursor() as cur:
        cur.execute("SELECT forecast.register_series('smoke_test', interval '15 minutes')")
        sid = cur.fetchone()[0]
        # Run + points in ONE transaction (skill rule), backtest-style claim.
        cur.execute(
            "INSERT INTO forecast.runs (run_name, model, available_at, context_start, context_end) "
            "VALUES ('smoke', 'constant', %s, %s, %s) RETURNING run_id",
            (SIM_AVAILABLE, T0 - timedelta(days=14), SIM_AVAILABLE),
        )
        run_id = cur.fetchone()[0]
        for i in range(4):
            cur.execute(
                "INSERT INTO forecast.forecasts (run_id, series_id, target_time, available_at, "
                "mean, q05, q10, q30, q50, q70, q90, q95) "
                "VALUES (%s, %s, %s, %s, 10, 5, 6, 8, 10, 12, 14, 15)",
                (run_id, sid, T0 + timedelta(minutes=15 * i), SIM_AVAILABLE),
            )
    conn.commit()

    with conn.cursor() as cur:
        sql, params = forecast_asof(config, "smoke_test", T0, T0 + timedelta(days=1), SIM_AVAILABLE)
        cur.execute(sql, params)
        assert len(cur.fetchall()) == 4

        # Vintage semantics: as-of before the claim sees nothing.
        sql, params = forecast_asof(
            config, "smoke_test", T0, T0 + timedelta(days=1), SIM_AVAILABLE - timedelta(hours=1)
        )
        cur.execute(sql, params)
        assert cur.fetchall() == []

        # recorded_at guards the claim: a backtest-style run is distinguishable.
        cur.execute(
            "SELECT recorded_at > available_at + interval '1 day' FROM forecast.runs "
            "WHERE run_id = %s",
            (run_id,),
        )
        assert cur.fetchone()[0]

        # The serving view speaks names.
        cur.execute(
            "SELECT series_name, q50 FROM forecast.latest_forecasts "
            "WHERE series_name = 'smoke_test' LIMIT 1"
        )
        assert cur.fetchone() == ("smoke_test", 10)


def test_hypertables_and_self_description(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
        if cur.fetchone() is None:
            pytest.skip("plain Postgres instance")
        cur.execute(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema = 'forecast' ORDER BY 1"
        )
        # Canonical tables are a subset: extra instances (workspaces,
        # per-band forecast logs) are legitimate on a shared store.
        assert {"actuals", "forecasts", "predictors"} <= {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT config->>'quantile_band' FROM forecast.store_tables "
            "WHERE table_name = 'forecasts'"
        )
        assert "0.95" in cur.fetchone()[0]


def test_config_from_store(conn):
    """StoreConfig.from_store rebuilds the declaration from store_tables alone
    (spec §5.2): the canonical switches round-trip, the result re-provisions
    as a no-op, and a missing store is named, not guessed."""
    from forecast_store.config import StoreConfig
    from forecast_store.provision import NotProvisioned, provision

    loaded = StoreConfig.from_store(conn)
    declared = StoreConfig()
    # Canonical switches round-trip. (Extra instances are covered in
    # test_multi_instance; a shared test store may legitimately carry some.)
    assert loaded.schema == "forecast"
    assert loaded.quantile_band == declared.quantile_band
    assert loaded.has_mean == declared.has_mean
    assert loaded.actuals_revisions == declared.actuals_revisions
    assert loaded.enforcement == declared.enforcement
    # provision() re-issues CREATE OR REPLACE VIEW (ACCESS EXCLUSIVE); end this
    # connection's read transaction first so its share locks cannot block it.
    conn.rollback()
    assert provision(DSN, loaded).already_provisioned  # drift-free by construction

    with pytest.raises(NotProvisioned, match="no_such_store"):
        StoreConfig.from_store(conn, schema="no_such_store")
    with pytest.raises(ValueError, match="identifier"):
        StoreConfig.from_store(conn, schema="Bad-Name; --")


def test_python_resolvers_and_series_refs(conn):
    """register_series / get_series_id are the SDK face of the SQL resolvers
    (schema from the config, UnknownSeries instead of a Postgres exception),
    and every read/write accepts the name or the id interchangeably."""
    from forecast_store.config import StoreConfig
    from forecast_store.read import read_context_series
    from forecast_store.series import UnknownSeries, get_series_id, register_series
    from forecast_store.write import write_forecast_run

    config = StoreConfig()
    sid = register_series(conn, config, "smoke_test", timedelta(minutes=15))
    assert register_series(conn, config, "smoke_test", "15 minutes") == sid  # get-or-create
    assert get_series_id(conn, config, "smoke_test") == sid
    with pytest.raises(UnknownSeries):
        get_series_id(conn, config, "smoke_test_typo")

    # Away from the other tests' target window; run label shared for cleanup.
    target = T0 + timedelta(days=2)
    common = dict(model="constant", run_name="smoke", points=[(target, {"q50": 1.0})])
    by_name = write_forecast_run(
        conn, config, series="smoke_test", available_at=SIM_AVAILABLE, **common
    )
    by_id = write_forecast_run(
        conn, config, series=sid, available_at=SIM_AVAILABLE + timedelta(hours=1),
        **{**common, "points": [(target, {"q50": 2.0})]},
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT series_id FROM forecast.forecasts WHERE run_id IN (%s, %s)",
            (by_name, by_id),
        )
        assert cur.fetchall() == [(sid,)]  # both spellings hit the same series
    for ref in ("smoke_test", sid):
        _, rows = read_context_series(
            conn, config, ref, table="forecasts", column="q50",
            start=target, end=target + timedelta(minutes=15),
            asof=SIM_AVAILABLE + timedelta(hours=1),
        )
        assert [v for _, _, v in rows] == [2.0]

    with pytest.raises(UnknownSeries):
        write_forecast_run(
            conn, config, series="smoke_test_typo", available_at=SIM_AVAILABLE, **common
        )
    conn.rollback()
    with pytest.raises(TypeError, match="registered name"):
        write_forecast_run(conn, config, series=1.5, available_at=SIM_AVAILABLE, **common)
    conn.rollback()
