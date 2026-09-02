"""One point shape on every write (DX item 3): ``(target_time, values)``, with
the knowledge time resolved by the table's stored declaration.

The DB-free tests drive the normalizer with declarations exactly as
``table_configs`` persists them; the live test round-trips through a store
carrying an extra, banded predictor instance.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

from forecast_store.config import PredictorLogSpec, StoreConfig
from forecast_store.ddl import table_configs
from forecast_store.write import _normalize

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

T0 = datetime(2024, 9, 1, tzinfo=timezone.utc)
H = timedelta(hours=1)
VENDOR = PredictorLogSpec("vendor_smoke", quantile_band=("0.5",))  # value + q50
DECL = table_configs(StoreConfig().with_tables(VENDOR))


def test_scalar_is_sugar_for_the_single_value_column():
    cols, rows = _normalize(
        "actuals", DECL["actuals"], [(T0, 1.0), (T0 + H, {"value": 2.0})],
        per_point_knowledge=True,
    )
    assert cols == ["value"]
    assert rows == [(T0, {"value": 1.0}), (T0 + H, {"value": 2.0})]


def test_scalar_rejected_where_the_column_is_ambiguous():
    with pytest.raises(ValueError, match="bare scalar"):
        _normalize("forecasts", DECL["forecasts"], [(T0, 1.0)], per_point_knowledge=False)
    with pytest.raises(ValueError, match="2 value columns.*bare scalar"):
        _normalize("vendor_smoke", DECL["vendor_smoke"], [(T0, 1.0)], per_point_knowledge=True)


def test_used_columns_follow_declaration_order():
    cols, _ = _normalize(
        "forecasts", DECL["forecasts"],
        [(T0, {"q95": 1.0}), (T0 + H, {"q05": 0.0, "mean": 0.5})],
        per_point_knowledge=False,
    )
    assert cols == ["mean", "q05", "q95"]


def test_unknown_columns_are_named_before_any_write():
    with pytest.raises(ValueError, match=r"\['q42'\] are not declared by 'forecasts'"):
        _normalize("forecasts", DECL["forecasts"], [(T0, {"q42": 1.0})], per_point_knowledge=False)
    # target_time_observed is writable only where the instance declares it.
    with pytest.raises(ValueError, match="target_time_observed"):
        _normalize(
            "actuals", DECL["actuals"],
            [(T0, {"value": 1.0, "target_time_observed": T0})],
            per_point_knowledge=True,
        )


def test_knowledge_column_is_per_point_except_on_forecast_logs():
    with pytest.raises(ValueError, match="knowledge time is the run's"):
        _normalize(
            "forecasts", DECL["forecasts"],
            [(T0, {"q50": 1.0, "available_at": T0})],
            per_point_knowledge=False,
        )
    cols, _ = _normalize(
        "vendor_smoke", DECL["vendor_smoke"],
        [(T0, {"q50": 1.0, "available_at": T0})],
        per_point_knowledge=True,
    )
    assert cols == ["q50", "available_at"]


def test_points_may_be_any_iterable():
    frame = {T0: {"value": 1.0}, T0 + H: {"value": 2.0}}  # e.g. df.to_dict("index")
    _, rows = _normalize("actuals", DECL["actuals"], frame.items(), per_point_knowledge=True)
    assert [ts for ts, _ in rows] == [T0, T0 + H]


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_point_shape_live():
    """One shape, three tables, knowledge time by declaration: per-row and
    batch claims on actuals (an unstated row is measured), batch publication
    on predictors, a banded vendor instance written by table name, and
    refusals that leave nothing behind."""
    psycopg = pytest.importorskip("psycopg")

    from forecast_store.provision import provision
    from forecast_store.read import read_context_series, read_versioned_series
    from forecast_store.series import register_series
    from forecast_store.write import write_actuals, write_forecast, write_predictors

    config = StoreConfig().with_tables(VENDOR)
    provision(DSN, config)
    name = "shape_smoke_series"
    with psycopg.connect(DSN) as conn:
        try:
            register_series(conn, config, name, H)
            with conn.cursor() as cur:
                cur.execute("SELECT now()")
                db_now = cur.fetchone()[0]

            # Actuals: a per-row claim, the batch claim, and an unstated row.
            write_actuals(
                conn, config, name,
                [(T0, {"value": 1.0, "available_at": T0 + H}), (T0 + H, 2.0)],
                available_at=T0 + 2 * H,
            )
            write_actuals(conn, config, name, [(T0 + 2 * H, 3.0)])
            conn.commit()
            _, rows = read_versioned_series(
                conn, config, name, table="actuals", start=T0, end=T0 + 3 * H
            )
            claims = {ts: avail for ts, avail, _ in rows}
            assert claims[T0] == T0 + H  # per-row claim wins
            assert claims[T0 + H] == T0 + 2 * H  # batch claim
            assert claims[T0 + 2 * H] >= db_now  # unstated: arrival measured

            # Predictors: one publication for a batch; a banded vendor instance by name.
            published = T0 - 6 * H
            write_predictors(
                conn, config, name,
                [(T0 + i * H, 10.0 + i) for i in range(3)], available_at=published,
            )
            write_predictors(
                conn, config, name,
                [
                    (T0, {"q50": 9.5, "value": 9.7}),
                    (T0 + H, {"q50": 10.5, "available_at": published + H}),  # later vintage
                ],
                available_at=published,
                table="vendor_smoke",
            )
            conn.commit()
            _, rows = read_context_series(
                conn, config, name, table="predictors",
                start=T0, end=T0 + 3 * H, asof=published,
            )
            assert [v for _, _, v in rows] == [10.0, 11.0, 12.0]
            window = dict(table="vendor_smoke", column="q50", start=T0, end=T0 + 2 * H)
            _, rows = read_context_series(conn, config, name, asof=published, **window)
            assert [v for _, _, v in rows] == [9.5, 9.5]  # later vintage invisible: locf
            _, rows = read_context_series(conn, config, name, asof=published + H, **window)
            assert [v for _, _, v in rows] == [9.5, 10.5]

            # Refusals: validated before any row lands.
            with pytest.raises(ValueError, match="knowledge time on every point"):
                write_predictors(conn, config, name, [(T0 + 5 * H, 1.0)])
            conn.rollback()
            with pytest.raises(ValueError, match="knowledge time is the run's"):
                write_forecast(
                    conn, config, series=name, model="m", available_at=T0,
                    points=[(T0, {"q50": 1.0, "available_at": T0})],
                )
            conn.rollback()
            with pytest.raises(ValueError, match="not a predictors instance"):
                write_predictors(conn, config, name, [(T0, 1.0)], available_at=T0, table="actuals")
            conn.rollback()
            with pytest.raises(ValueError, match="bare scalar"):
                write_predictors(
                    conn, config, name, [(T0, 1.0)], available_at=T0, table="vendor_smoke"
                )
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM forecast.predictors "
                    "WHERE series_id = forecast.get_series_id(%s)", (name,),
                )
                assert cur.fetchone()[0] == 3
        finally:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
                )
                cur.execute("DROP VIEW IF EXISTS forecast.latest_vendor_smoke")
                cur.execute("DROP TABLE IF EXISTS forecast.vendor_smoke")
                cur.execute("DELETE FROM forecast.store_tables WHERE table_name = 'vendor_smoke'")
                for table in ("actuals", "predictors"):
                    cur.execute(
                        f"DELETE FROM forecast.{table} WHERE series_id IN "
                        "(SELECT series_id FROM forecast.series WHERE name = %s)", (name,),
                    )
                cur.execute("DELETE FROM forecast.series WHERE name = %s", (name,))
            conn.commit()
