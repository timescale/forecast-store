"""Multi-instance stores: tables beyond the conventional trio, declared with
StoreConfig.with_tables (or any flat ``tables=`` set).

Unit tests cover generation; the live test provisions a workspace forecast log
*additively* onto the existing store (instances arrive as additions, spec
§5.2/§7.2), writes and reads through it by table name, and cleans it up.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

from forecast_store.config import (
    ActualsSpec,
    ForecastLogSpec,
    PredictorLogSpec,
    StoreConfig,
)
from forecast_store.ddl import generate_ddl, hypertable_ddl, table_configs

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

EXTRAS = (
    ForecastLogSpec("bt_workspace", quantile_band=("0.1", "0.5", "0.9")),
    PredictorLogSpec("vendor_x"),
    ActualsSpec("tenant_a_actuals", revisions=False),
)


def test_extra_instances_generate_everything():
    config = StoreConfig().with_tables(*EXTRAS)
    ddl = "\n".join(generate_ddl(config))
    # Workspace forecast log: own band columns, index, serving view.
    assert "CREATE TABLE IF NOT EXISTS forecast.bt_workspace" in ddl
    for col in ("mean", "q10", "q50", "q90"):
        assert f"{col} " in ddl.split("bt_workspace")[1].split(")")[0] or col in ddl
    assert "bt_workspace_asof_idx" in ddl
    assert "VIEW forecast.latest_bt_workspace" in ddl
    # Predictor and single-belief actuals instances.
    assert "CREATE TABLE IF NOT EXISTS forecast.vendor_x" in ddl
    assert "CREATE TABLE IF NOT EXISTS forecast.tenant_a_actuals" in ddl

    configs = table_configs(config)
    assert configs["bt_workspace"] == {
        "role": "forecasts",
        "value_columns": ["mean", "q10", "q50", "q90"],
        "knowledge_column": "available_at",
        "has_runs": True,
        "enforcement": "monitor",
        "quantile_band": ["0.1", "0.5", "0.9"],
        "has_mean": True,
    }
    assert configs["vendor_x"]["role"] == "predictors"
    assert configs["tenant_a_actuals"]["knowledge_column"] == "available_at"  # both shapes
    assert configs["tenant_a_actuals"]["revisions"] is False  # the PK switch

    hyper = "\n".join(hypertable_ddl(config))
    for table in ("bt_workspace", "vendor_x", "tenant_a_actuals"):
        assert f"create_hypertable('forecast.{table}'" in hyper
        assert f"add_columnstore_policy('forecast.{table}'" in hyper


def test_banded_predictor_instance():
    """A probabilistic vendor feed: band on a predictors-shaped instance."""
    config = StoreConfig().with_tables(
        PredictorLogSpec("vendor_prob", quantile_band=("0.1", "0.5", "0.9"))
    )
    declaration = table_configs(config)["vendor_prob"]
    assert declaration["value_columns"] == ["value", "q10", "q50", "q90"]
    assert declaration["quantile_band"] == ["0.1", "0.5", "0.9"]
    assert declaration["has_runs"] is False  # provenance is the discriminator
    ddl = "\n".join(generate_ddl(config))
    block = ddl.split("vendor_prob (")[1].split("PRIMARY KEY")[0]
    for col in ("value", "q10", "q50", "q90"):
        assert f"{col} " in block
    assert "run_id" not in block  # a banded predictor is still not a forecast log
    with pytest.raises(ValueError, match="at least one value column"):
        PredictorLogSpec("empty", has_value=False)


def test_instance_validation():
    with pytest.raises(ValueError, match="reserved"):
        ActualsSpec("runs")  # infrastructure names are the only reserved ones
    with pytest.raises(ValueError, match="duplicate"):
        StoreConfig().with_tables(PredictorLogSpec("x"), PredictorLogSpec("x"))
    with pytest.raises(ValueError, match="identifier"):
        PredictorLogSpec("Bad-Name")
    # A forecast log's default band is the reference band — the same as `forecasts`.
    config = StoreConfig().with_tables(ForecastLogSpec("wide"))
    assert (
        table_configs(config)["wide"]["value_columns"]
        == list(config.table("forecasts").value_columns)
    )


def test_config_round_trips_through_store_tables():
    """config_from_tables inverts table_configs (spec §5.2): what a store
    persists is enough to rebuild the declaration it was built from — every
    switch, every extra instance — so clients need not redeclare it."""
    from forecast_store.ddl import config_from_tables

    config = StoreConfig.standard(
        ["0.1", "0.5", "0.9"],
        has_mean=False,
        actuals_revisions=False,
        schema="fs_rt",
        enforcement="fk",
        append_only_guard=True,
    ).with_tables(
        PredictorLogSpec("vendor_x", quantile_band=("0.25", "0.75"), has_value=False),
        ForecastLogSpec("bt_workspace", quantile_band=["0.1", "0.5", "0.9"]),
        ActualsSpec("sensor_a", has_target_time_observed=True),
        ActualsSpec("meter_sb", revisions=False),
    )
    rebuilt = config_from_tables(table_configs(config), schema="fs_rt", append_only_guard=True)
    assert rebuilt == config
    assert table_configs(rebuilt) == table_configs(config)
    # Tables are canonicalized to name order: declaration order never breaks equality.
    assert config.table_names == (
        "actuals", "bt_workspace", "forecasts", "meter_sb", "predictors", "sensor_a", "vendor_x",
    )

    # No table is implied: a single-table store rebuilds as exactly that.
    assert config_from_tables({"actuals": {"role": "actuals"}}) == StoreConfig(
        tables=(ActualsSpec("actuals"),)
    )
    with pytest.raises(ValueError, match="unknown role"):
        config_from_tables({**table_configs(StoreConfig()), "odd": {"role": "mystery"}})


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_workspace_instance_live():
    """Additive provision of a workspace forecast log onto the existing store,
    write/read through it by table name, canonical tables untouched."""
    psycopg = pytest.importorskip("psycopg")

    from forecast_store.provision import provision
    from forecast_store.read import read_context_series
    from forecast_store.series import register_series
    from forecast_store.write import write_forecast

    config = StoreConfig().with_tables(
        ForecastLogSpec("bt_workspace", quantile_band=("0.1", "0.5", "0.9"))
    )
    report = provision(DSN, config)  # additive onto the already-provisioned store
    assert report.already_provisioned

    t0 = datetime(2024, 9, 1, tzinfo=timezone.utc)
    sim = t0 - timedelta(hours=12)
    with psycopg.connect(DSN) as conn:
        try:
            # The instance — band and all — is recoverable from the store alone.
            loaded = StoreConfig.from_store(conn)
            assert loaded.table("bt_workspace") == config.table("bt_workspace")
            sid = register_series(conn, config, "mi_smoke", timedelta(hours=1))
            write_forecast(
                conn,
                config,
                table="bt_workspace",
                series="mi_smoke",
                model="constant",
                run_name="mi_smoke_run",
                available_at=sim,
                points=[(t0 + i * timedelta(hours=1), {"q50": 7.0}) for i in range(4)],
            )
            conn.commit()

            # Canonical column set is rejected by the instance's declaration.
            with pytest.raises(ValueError, match="not declared by 'bt_workspace'"):
                write_forecast(
                    conn, config, table="bt_workspace", series=sid,
                    model="constant", available_at=sim,
                    points=[(t0, {"q05": 1.0})],
                )
            conn.rollback()

            # Read back through the table-name API and the generated view.
            _, rows = read_context_series(
                conn, config, "mi_smoke",
                table="bt_workspace", column="q50",
                start=t0, end=t0 + timedelta(hours=4), asof=sim,
            )
            assert [v for _, _, v in rows] == [7.0] * 4
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT q50 FROM forecast.latest_bt_workspace WHERE series_name = 'mi_smoke'"
                )
                assert cur.fetchone()[0] == 7.0
                # Canonical forecasts table untouched by the workspace write.
                cur.execute(
                    "SELECT count(*) FROM forecast.forecasts WHERE series_id = %s", (sid,)
                )
                assert cur.fetchone()[0] == 0
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
                )
                cur.execute("DELETE FROM forecast.runs WHERE run_name = 'mi_smoke_run'")
                cur.execute("DROP VIEW IF EXISTS forecast.latest_bt_workspace")
                cur.execute("DROP TABLE IF EXISTS forecast.bt_workspace")
                cur.execute("DELETE FROM forecast.store_tables WHERE table_name = 'bt_workspace'")
                cur.execute("DELETE FROM forecast.series WHERE name = 'mi_smoke'")
            conn.commit()

def test_observed_actuals_instance():
    """spec §6.1: optional nullable target_time_observed on an actuals instance."""
    config = StoreConfig().with_tables(ActualsSpec("sensor_a", has_target_time_observed=True))
    ddl = "\n".join(generate_ddl(config))
    block = ddl.split("sensor_a (")[1].split("PRIMARY KEY")[0]
    assert "target_time_observed timestamptz," in block  # nullable, never defaulted
    declarations = table_configs(config)
    assert declarations["sensor_a"]["has_target_time_observed"] is True
    # Only-when-true: canonical actuals declaration is unchanged (drift-free).
    assert "has_target_time_observed" not in declarations["actuals"]
    # The sweep is catalog-driven: it never names instances — the stored
    # declaration (has_target_time_observed) gates the observed check at run
    # time, so provisioning the instance is all it takes to be swept.
    sweep = next(s for s in hypertable_ddl(config) if "data_quality_sweep" in s)
    assert "observed_outside_bucket" in sweep
    assert "sensor_a" not in sweep
    assert "has_target_time_observed" in sweep


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_observed_instance_and_grid_validation_live():
    """Observed column round-trips; off-grid writes fail loud; sweep flags
    what raw-SQL writers sneak past the SDK."""
    psycopg = pytest.importorskip("psycopg")

    from forecast_store.provision import provision
    from forecast_store.series import register_series
    from forecast_store.write import MisalignedTimestamp, write_actuals

    config = StoreConfig().with_tables(ActualsSpec("obs_smoke", has_target_time_observed=True))
    provision(DSN, config)  # additive; the catalog-driven sweep needs no regeneration

    t0 = datetime(2024, 9, 1, tzinfo=timezone.utc)
    jitter = timedelta(seconds=3, milliseconds=200)
    with psycopg.connect(DSN) as conn:
        try:
            sid = register_series(conn, config, "obs_smoke_series", timedelta(minutes=15))
            write_actuals(
                conn, config, sid,
                [(t0, {"value": 1.0, "target_time_observed": t0 + jitter}),
                 (t0 + timedelta(minutes=15), 2.0)],  # scalar: the single value column
                table="obs_smoke",
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT target_time_observed FROM forecast.obs_smoke "
                    "WHERE series_id = %s ORDER BY target_time", (sid,),
                )
                observed = [r[0] for r in cur.fetchall()]
            assert observed == [t0 + jitter, None]

            # Off-grid target_time fails loud at the SDK write path (spec §5.1).
            with pytest.raises(MisalignedTimestamp):
                write_actuals(
                    conn, config, sid,
                    [(t0 + timedelta(seconds=7), 3.0)], table="obs_smoke",
                )
            conn.rollback()

            # Observed timestamps on an instance that never declared the column.
            with pytest.raises(ValueError, match="target_time_observed"):
                write_actuals(
                    conn, config, sid, [(t0, {"value": 1.0, "target_time_observed": t0})]
                )
            conn.rollback()

            # Raw SQL sneaks an out-of-bucket observation past the SDK;
            # the generated sweep is the backstop (spec §8).
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO forecast.obs_smoke "
                    "(series_id, target_time, target_time_observed, value) "
                    "VALUES (%s, %s, %s, %s)",
                    (sid, t0 + timedelta(minutes=30), t0, 9.0),
                )
                cur.execute(
                    "SELECT issue, n FROM forecast.data_quality_sweep('1 hour') "
                    "WHERE series_id = %s", (sid,),
                )
                issues = dict(cur.fetchall())
            assert issues.get("observed_outside_bucket") == 1
            conn.rollback()
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
                )
                cur.execute("DROP TABLE IF EXISTS forecast.obs_smoke")
                cur.execute("DELETE FROM forecast.store_tables WHERE table_name = 'obs_smoke'")
                cur.execute("DELETE FROM forecast.series WHERE name = 'obs_smoke_series'")
            conn.commit()
    # No restore needed: the catalog-driven sweep forgets obs_smoke the moment
    # its store_tables row is deleted above — that is the point of the design.


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_single_belief_instance_live():
    """Single-belief live: stated claims legal, identical re-delivery idempotent
    (first claim wins), conflicting value raises ConflictingBelief."""
    psycopg = pytest.importorskip("psycopg")

    from forecast_store.provision import provision
    from forecast_store.series import register_series
    from forecast_store.write import ConflictingBelief, write_actuals

    config = StoreConfig().with_tables(ActualsSpec("sb_smoke", revisions=False))
    provision(DSN, config)

    t0 = datetime(2024, 9, 1, tzinfo=timezone.utc)
    claim = t0 + timedelta(hours=1)  # genuine historical arrival, stated
    with psycopg.connect(DSN) as conn:
        try:
            sid = register_series(conn, config, "sb_smoke_series", timedelta(minutes=15))
            write_actuals(
                conn, config, sid,
                [(t0, 1.0), (t0 + timedelta(minutes=15), 2.0)],
                available_at=claim, table="sb_smoke",
            )
            conn.commit()
            # Identical re-delivery, claim omitted: silent no-op, stored claim wins.
            write_actuals(conn, config, sid, [(t0, 1.0)], table="sb_smoke")
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*), min(available_at) FROM forecast.sb_smoke "
                    "WHERE series_id = %s", (sid,),
                )
                n, first_claim = cur.fetchone()
            assert n == 2 and first_claim == claim
            # A different value for an existing target: never silently swallowed.
            with pytest.raises(ConflictingBelief, match="conflicting belief"):
                write_actuals(conn, config, sid, [(t0, 9.0)], table="sb_smoke")
            conn.rollback()
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
                )
                cur.execute("DROP TABLE IF EXISTS forecast.sb_smoke")
                cur.execute("DELETE FROM forecast.store_tables WHERE table_name = 'sb_smoke'")
                cur.execute("DELETE FROM forecast.series WHERE name = 'sb_smoke_series'")
            conn.commit()
