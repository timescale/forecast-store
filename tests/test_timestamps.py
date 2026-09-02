"""Naive datetimes are refused before any statement runs (DX item 9), on
every write and read path and in the query builder."""

import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from forecast_store import ActualsSpec, NaiveTimestamp, Store, StoreConfig, forecast_asof
from forecast_store.ddl import table_configs
from forecast_store.errors import ForecastStoreError
from forecast_store.read import read_context_series, read_versioned_series
from forecast_store.timestamps import aware
from forecast_store.write import _normalize, write_actuals, write_forecast_run, write_predictors

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

UTC_T = datetime(2024, 1, 1, tzinfo=timezone.utc)
NAIVE_T = datetime(2024, 1, 1)
H = timedelta(hours=1)
CFG = StoreConfig()
DECL = table_configs(CFG)


class _NoDB:
    """A connection stand-in that fails the test if anything reaches it."""

    def cursor(self):
        raise AssertionError("touched the database before validating timestamps")


def test_aware_accepts_any_real_zone_and_refuses_naive():
    for ok in (UTC_T, UTC_T.astimezone(ZoneInfo("Europe/Amsterdam")),
               datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=-5)))):
        assert aware(ok, "t") is ok
    with pytest.raises(NaiveTimestamp, match="asof .* is naive") as info:
        aware(NAIVE_T, "asof")
    assert isinstance(info.value, ValueError) and isinstance(info.value, ForecastStoreError)
    with pytest.raises(TypeError, match="must be a datetime"):
        aware(date(2024, 1, 1), "t")
    with pytest.raises(TypeError):
        aware("2024-01-01T00:00:00Z", "t")


def test_pandas_timestamps_count_as_datetimes():
    pd = pytest.importorskip("pandas")
    assert aware(pd.Timestamp("2024-01-01", tz="UTC"), "t") is not None
    with pytest.raises(NaiveTimestamp):
        aware(pd.Timestamp("2024-01-01"), "t")


def test_points_are_checked_field_by_field():
    with pytest.raises(NaiveTimestamp, match="target_time"):
        _normalize("actuals", DECL["actuals"], [(NAIVE_T, 1.0)], per_point_knowledge=True)
    with pytest.raises(NaiveTimestamp, match="available_at"):
        _normalize(
            "actuals", DECL["actuals"],
            [(UTC_T, {"value": 1.0, "available_at": NAIVE_T})], per_point_knowledge=True,
        )
    observed = table_configs(
        StoreConfig(extra_tables=(ActualsSpec("obs", has_target_time_observed=True),))
    )["obs"]
    with pytest.raises(NaiveTimestamp, match="target_time_observed"):
        _normalize(
            "obs", observed,
            [(UTC_T, {"value": 1.0, "target_time_observed": NAIVE_T})], per_point_knowledge=True,
        )


def test_writes_refuse_before_touching_the_database():
    with pytest.raises(NaiveTimestamp, match="available_at"):
        write_forecast_run(_NoDB(), CFG, series="s", model="m", points=[], available_at=NAIVE_T)
    with pytest.raises(NaiveTimestamp, match="context_end"):
        write_forecast_run(
            _NoDB(), CFG, series="s", model="m", points=[], available_at=UTC_T,
            context_end=NAIVE_T,
        )
    with pytest.raises(NaiveTimestamp, match="available_at"):
        write_actuals(_NoDB(), CFG, "s", [], available_at=NAIVE_T)
    with pytest.raises(NaiveTimestamp, match="available_at"):
        write_predictors(_NoDB(), CFG, "s", [], available_at=NAIVE_T)


def test_reads_and_the_query_builder_refuse_before_touching_the_database():
    with pytest.raises(NaiveTimestamp, match="asof"):
        read_context_series(
            _NoDB(), CFG, "s", table="actuals", start=UTC_T, end=UTC_T + H, asof=NAIVE_T
        )
    with pytest.raises(NaiveTimestamp, match="recorded_before"):
        read_context_series(
            _NoDB(), CFG, "s", table="actuals", start=UTC_T, end=UTC_T + H, asof=UTC_T,
            recorded_before=NAIVE_T,
        )
    with pytest.raises(NaiveTimestamp, match="start"):
        read_versioned_series(_NoDB(), CFG, "s", table="actuals", start=NAIVE_T, end=UTC_T)
    with pytest.raises(NaiveTimestamp, match="target_end"):
        forecast_asof(CFG, "s", UTC_T, NAIVE_T, UTC_T)


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_store_refuses_naive_live():
    with Store.connect(DSN) as store:
        with pytest.raises(NaiveTimestamp):
            store.read_context_series(
                "any-name", table="actuals", start=NAIVE_T, end=UTC_T, asof=UTC_T
            )
        with pytest.raises(NaiveTimestamp):  # call-level claim, checked before the lookup
            store.write_actuals("any-name", [(UTC_T, 1.0)], available_at=NAIVE_T)
