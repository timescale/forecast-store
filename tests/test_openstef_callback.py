"""Phase A live test: a real OpenSTEF workflow writes through ForecastStoreCallback.

Requires FORECAST_STORE_TEST_DSN and the openstef extra
(``uv run --extra dev --extra openstef pytest``); skipped otherwise.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

pytestmark = pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")

SERIES = "ph_a_smoke"
RUN_NAME = "ph_a"


@pytest.fixture(scope="module")
def store_conn():
    psycopg = pytest.importorskip("psycopg")

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
            "(SELECT run_id FROM forecast.runs WHERE run_name = %s)",
            (RUN_NAME,),
        )
        cur.execute("DELETE FROM forecast.runs WHERE run_name = %s", (RUN_NAME,))
        cur.execute("DELETE FROM forecast.series WHERE name = %s", (SERIES,))
    conn.commit()


@pytest.fixture(scope="module")
def prediction(store_conn):
    """Run a real OpenSTEF workflow with the callback attached; return (callback, result)."""
    pytest.importorskip("openstef_models")
    import numpy as np
    import pandas as pd
    from openstef_core.datasets import TimeSeriesDataset
    from openstef_core.types import LeadTime, Q
    from openstef_models.models.forecasting.constant_quantile_forecaster import (
        ConstantQuantileForecaster,
    )
    from openstef_models.models.forecasting_model import ForecastingModel
    from openstef_models.workflows.custom_forecasting_workflow import (
        CustomForecastingWorkflow,
    )

    from forecast_store.integrations.openstef import ForecastStoreCallback

    rng = np.random.default_rng(42)
    index = pd.date_range("2025-01-01", periods=48, freq="h", tz="UTC")
    dataset = TimeSeriesDataset(
        data=pd.DataFrame(
            {"load": rng.standard_normal(48), "temperature": rng.standard_normal(48)},
            index=index,
        ),
        sample_interval=timedelta(hours=1),
    )

    model = ForecastingModel(
        forecaster=ConstantQuantileForecaster(
            horizons=[LeadTime.from_string("PT24H")],
            quantiles=[Q(0.1), Q(0.5), Q(0.9)],  # ⊆ the store's liander band
        )
    )
    callback = ForecastStoreCallback(DSN, SERIES)
    workflow = CustomForecastingWorkflow(
        model=model, model_id="ph_a_model", run_name=RUN_NAME, callbacks=[callback]
    )
    workflow.fit(dataset)
    result = workflow.predict(dataset)
    assert callback.last_run_id is not None
    return callback, result, dataset


def test_points_round_trip(store_conn, prediction):
    callback, result, _ = prediction
    with store_conn.cursor() as cur:
        cur.execute(
            "SELECT target_time, q10, q50, q90 FROM forecast.forecasts "
            "WHERE run_id = %s ORDER BY target_time",
            (callback.last_run_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == len(result.data)
    stored_q50 = {ts: q50 for ts, _, q50, _ in rows}
    for ts, row in result.data.iterrows():
        assert stored_q50[ts.to_pydatetime()] == pytest.approx(row["quantile_P50"])


def test_run_provenance(store_conn, prediction):
    callback, result, dataset = prediction
    with store_conn.cursor() as cur:
        cur.execute(
            "SELECT model, run_name, available_at, recorded_at, context_start, context_end, params "
            "FROM forecast.runs WHERE run_id = %s",
            (callback.last_run_id,),
        )
        model, run_name, available_at, recorded_at, ctx_start, ctx_end, params = cur.fetchone()

    assert model == "ConstantQuantileForecaster"
    assert run_name == RUN_NAME
    # Real knowledge time: claim ≈ measurement (production shape, spec §4.1).
    assert abs((recorded_at - available_at).total_seconds()) < 60
    # Context window: bounds of the observed target history.
    assert ctx_start == dataset.data.index.min().to_pydatetime()
    assert ctx_end == dataset.data["load"].last_valid_index().to_pydatetime()
    # Leakage audit passes: context_end <= available_at.
    assert ctx_end <= available_at
    # As-used snapshot.
    assert params["engine"] == "openstef"
    assert params["model_id"] == "ph_a_model"
    assert params["quantile_columns"] == {
        "quantile_P10": "q10",
        "quantile_P50": "q50",
        "quantile_P90": "q90",
    }
    assert params["context_end_method"] == "last_observed_target"


def test_series_auto_registered_with_explicit_interval(store_conn, prediction):
    with store_conn.cursor() as cur:
        cur.execute(
            "SELECT sample_interval FROM forecast.series WHERE name = %s", (SERIES,)
        )
        (interval,) = cur.fetchone()
    assert interval == timedelta(hours=1)


def test_band_mismatch_rejected(store_conn, prediction):
    """Connector policy: quantiles outside the declared band error, never write."""
    pytest.importorskip("openstef_models")
    import pandas as pd
    from openstef_core.datasets.validated_datasets import ForecastDataset

    from forecast_store.integrations.openstef import ForecastStoreCallback

    callback = ForecastStoreCallback(DSN, SERIES)
    off_band = ForecastDataset(
        pd.DataFrame(
            {"quantile_P2.5": [1.0], "quantile_P97.5": [2.0]},
            index=pd.date_range("2025-01-01", periods=1, freq="h", tz="UTC"),
        ),
        sample_interval=timedelta(hours=1),
        forecast_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    with pytest.raises(ValueError, match="not in the store's declared band"):
        callback._quantile_column_map(off_band)
