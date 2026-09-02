"""Read results are small named tuples (DX item 8): they still unpack as
``(sample_interval, rows)`` and add ``.gaps`` and a UTC-canonical
``.to_pandas()``."""

import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from forecast_store import ContextSeries, Store, VersionedSeries

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

H = timedelta(hours=1)
T0 = datetime(2024, 11, 1, tzinfo=ZoneInfo("Etc/UTC"))  # psycopg's usual tz spelling
SERIES = "read_results_smoke"


def test_context_series_unpacks_counts_gaps_and_converts_to_utc_pandas():
    pd = pytest.importorskip("pandas")
    rows = [(T0, 1.0, 1.0), (T0 + H, None, 1.0), (T0 + 2 * H, 3.0, 3.0)]
    result = ContextSeries(H, rows)

    interval, unpacked = result  # the old shape still works
    assert (interval, unpacked) == (H, rows)
    assert result.gaps == 1

    series = result.to_pandas()
    assert isinstance(series, pd.Series) and series.name == "value"
    assert str(series.index.tz) == "UTC" and series.index.name == "target_time"
    assert series.dtype == float and list(series) == [1.0, 1.0, 3.0]
    assert series.loc[datetime(2024, 11, 1, 1, tzinfo=timezone.utc)] == 1.0  # locf-filled bucket

    empty = ContextSeries(H, []).to_pandas()
    assert len(empty) == 0 and str(empty.index.tz) == "UTC"


def test_versioned_series_frame_is_utc_canonical():
    pd = pytest.importorskip("pandas")
    offset = timezone(timedelta(hours=2))  # a fixed-offset claim, as a client might send
    rows = [(T0, T0 - H, 1.0), (T0, T0.astimezone(offset), 1.5), (T0 + H, T0, None)]
    result = VersionedSeries(H, rows)

    assert tuple(result) == (H, rows)
    frame = result.to_pandas()
    assert list(frame.columns) == ["target_time", "available_at", "value"]
    assert str(frame["target_time"].dt.tz) == "UTC" and str(frame["available_at"].dt.tz) == "UTC"
    assert frame["available_at"].iloc[1] == pd.Timestamp(T0)  # same instant, now spelled in UTC
    assert frame["value"].dtype == float and frame["value"].isna().sum() == 1


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_reads_return_the_result_types_live():
    psycopg = pytest.importorskip("psycopg")
    pytest.importorskip("pandas")
    from forecast_store import provision

    provision(DSN)
    _cleanup(psycopg)
    try:
        with Store.connect(DSN) as store:
            store.register_series(SERIES, H)
            # T0 + 1h missing; claims stated at T0 so a 2024 as-of cutoff can see them
            store.write_actuals(SERIES, [(T0, 10.0), (T0 + 2 * H, 12.0)], available_at=T0)
            context = store.read_context_series(
                SERIES, table="actuals", start=T0, end=T0 + 3 * H, asof=T0 + 3 * H
            )
            assert isinstance(context, ContextSeries) and context.gaps == 1
            assert list(context.to_pandas()) == [10.0, 10.0, 12.0]
            versioned = store.read_versioned_series(
                SERIES, table="actuals", start=T0, end=T0 + 3 * H
            )
            assert isinstance(versioned, VersionedSeries)
            frame = versioned.to_pandas()
            assert list(frame["value"]) == [10.0, 12.0]
            assert str(frame["available_at"].dt.tz) == "UTC"
    finally:
        _cleanup(psycopg)


def _cleanup(psycopg):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        cur.execute(
            "DELETE FROM forecast.actuals WHERE series_id IN "
            "(SELECT series_id FROM forecast.series WHERE name = %s)", (SERIES,),
        )
        cur.execute("DELETE FROM forecast.series WHERE name = %s", (SERIES,))
