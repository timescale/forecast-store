"""forecast_asof at the same level as the reads (DX item 10): a DB-free
builder that resolves the table from the declaration and takes the pins the
reads take, plus the executed form on Store returning ForecastsAsOf."""

import os
from datetime import datetime, timedelta, timezone

import pytest

from forecast_store import (
    DeclarationMismatch,
    ForecastLogSpec,
    ForecastsAsOf,
    Store,
    StoreConfig,
    UnknownTable,
    forecast_asof,
    forecast_asof_columns,
)

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

T0 = datetime(2024, 12, 1, tzinfo=timezone.utc)
H = timedelta(hours=1)
WS = ForecastLogSpec("asof_ws", quantile_band=("0.5",), has_mean=False)
CFG = StoreConfig(extra_tables=(WS,))
NAME = "asof_smoke_series"
KEYS = ("series_id", "target_time", "available_at", "run_id")


def test_default_shape_is_unchanged():
    sql, params = forecast_asof(CFG, "s", T0, T0 + H, T0)
    assert "FROM forecast.forecasts f\n" in sql
    assert "JOIN" not in sql and "recorded_at" not in sql
    assert "f.series_id = forecast.get_series_id(%s)" in sql
    assert params == ("s", T0, T0 + H, T0)
    assert forecast_asof_columns(CFG) == KEYS + CFG.value_columns


def test_table_resolves_from_the_declaration():
    sql, _ = forecast_asof(CFG, "s", T0, T0 + H, T0, table="asof_ws")
    assert "FROM forecast.asof_ws f" in sql
    assert "f.run_id, f.q50\n" in sql and "f.mean" not in sql
    assert forecast_asof_columns(CFG, "asof_ws") == KEYS + ("q50",)
    with pytest.raises(UnknownTable):
        forecast_asof(CFG, "s", T0, T0 + H, T0, table="nope")
    for not_a_log in ("actuals", "predictors", "evaluation_runs"):
        with pytest.raises(DeclarationMismatch, match="not a forecast log"):
            forecast_asof(CFG, "s", T0, T0 + H, T0, table=not_a_log)


def test_pins_add_predicates_in_parameter_order():
    pin = T0 + 2 * H
    sql, params = forecast_asof(CFG, "s", T0, T0 + H, T0, recorded_before=pin, run_name="job")
    assert "f.recorded_at <= %s" in sql
    assert "JOIN forecast.runs r ON r.run_id = f.run_id" in sql and "r.run_name = %s" in sql
    assert params == ("s", T0, T0 + H, T0, pin, "job")


def test_series_id_is_a_direct_predicate():
    sql, params = forecast_asof(CFG, 42, T0, T0 + H, T0)
    assert "f.series_id = %s" in sql and "get_series_id" not in sql
    assert params[0] == 42
    with pytest.raises(TypeError):
        forecast_asof(CFG, True, T0, T0 + H, T0)


def test_result_frame():
    pytest.importorskip("pandas")
    result = ForecastsAsOf(forecast_asof_columns(CFG, "asof_ws"), [(7, T0, T0 - H, "run-1", 1.5)])
    columns, rows = result
    assert columns[-1] == "q50" and rows[0][-1] == 1.5
    frame = result.to_pandas()
    assert list(frame.columns) == list(columns)
    assert str(frame["target_time"].dt.tz) == "UTC" and frame["q50"].iloc[0] == 1.5


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_forecast_asof_live():
    """Vintage selection with every pin: the system clock excludes a later
    write, run_name excludes another producer, table= reaches a workspace
    instance, and name and id agree."""
    psycopg = pytest.importorskip("psycopg")
    from forecast_store import provision

    provision(DSN, CFG)
    _cleanup(psycopg, drop=False)
    try:
        with Store.connect(DSN, CFG) as store:
            store.register_series(NAME, H)
            first = store.write_forecast_run(
                series=NAME, model="m", run_name="asof/a", available_at=T0 - 2 * H,
                points=[(T0, {"q50": 1.0})],
            )
            # recorded_at is transaction time: end this transaction, take the
            # clock in the next, end that too, so the second write is later.
            store.conn.commit()
            with store.conn.cursor() as cur:
                cur.execute("SELECT clock_timestamp()")
                between = cur.fetchone()[0]
            store.conn.commit()
            second = store.write_forecast_run(
                series=NAME, model="m", run_name="asof/b", available_at=T0 - H,
                points=[(T0, {"q50": 2.0})],
            )
            store.write_forecast_run(
                series=NAME, model="m", run_name="asof/a", available_at=T0 - H,
                table="asof_ws", points=[(T0, {"q50": 9.0})],
            )
            store.conn.commit()

            latest = store.forecast_asof(NAME, T0, T0 + H, T0)
            run_id, q50 = latest.columns.index("run_id"), latest.columns.index("q50")
            assert [r[run_id] for r in latest.rows] == [second]
            assert latest.rows[0][q50] == 2.0

            pinned = store.forecast_asof(NAME, T0, T0 + H, T0, recorded_before=between)
            assert [r[run_id] for r in pinned.rows] == [first]  # later write invisible
            producer = store.forecast_asof(NAME, T0, T0 + H, T0, run_name="asof/a")
            assert [r[run_id] for r in producer.rows] == [first]
            ws = store.forecast_asof(NAME, T0, T0 + H, T0, table="asof_ws")
            assert ws.columns == KEYS + ("q50",) and [r[-1] for r in ws.rows] == [9.0]
            by_id = store.forecast_asof(store.get_series_id(NAME), T0, T0 + H, T0)
            assert by_id.rows == latest.rows
            assert list(latest.to_pandas()["q50"]) == [2.0]
    finally:
        _cleanup(psycopg, drop=True)


def _cleanup(psycopg, *, drop):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        for table in ("forecasts", "asof_ws"):
            cur.execute(
                f"DELETE FROM forecast.{table} WHERE run_id IN "
                "(SELECT run_id FROM forecast.runs WHERE run_name LIKE %s)", ("asof/%",),
            )
        cur.execute("DELETE FROM forecast.runs WHERE run_name LIKE %s", ("asof/%",))
        cur.execute("DELETE FROM forecast.series WHERE name = %s", (NAME,))
        if drop:
            cur.execute("DROP VIEW IF EXISTS forecast.latest_asof_ws")
            cur.execute("DROP TABLE IF EXISTS forecast.asof_ws")
            cur.execute("DELETE FROM forecast.store_tables WHERE table_name = 'asof_ws'")
