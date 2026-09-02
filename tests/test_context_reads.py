"""Phase A live test: store-served context assembly.

Seeds actuals + weather-forecast vintages, assembles a model input through
StoreReader (as-of reads, gapfill on the declared grid), runs a real OpenSTEF
workflow on it, and verifies the completed leakage audit — including that a
vintage published *after* the decision moment is invisible to the read.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

pytestmark = pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")

LOAD, TEMP = "ph_a_ctx_load", "ph_a_ctx_temp"
RUN_NAME = "ph_a_ctx"
ASOF = datetime(2025, 2, 3, tzinfo=timezone.utc)
HISTORY_START = ASOF - timedelta(hours=48)
HORIZON_END = ASOF + timedelta(hours=24)
HOUR = timedelta(hours=1)
GAP_TS = HISTORY_START + 20 * HOUR  # one deliberately missing measurement
LATE_VINTAGE_AT = ASOF + HOUR  # published after the decision moment


@pytest.fixture(scope="module")
def seeded(request):
    psycopg = pytest.importorskip("psycopg")
    pytest.importorskip("openstef_models")

    from forecast_store.config import StoreConfig
    from forecast_store.provision import provision
    from forecast_store.series import register_series
    from forecast_store.write import write_actuals, write_predictors

    config = StoreConfig()
    provision(DSN, config)
    conn = psycopg.connect(DSN)
    request.addfinalizer(conn.close)
    _cleanup(conn)
    request.addfinalizer(lambda: _cleanup(conn))

    register_series(conn, config, LOAD, HOUR)
    register_series(conn, config, TEMP, HOUR)

    # Measured history: hourly, one gap; backfill-style explicit claims.
    history = [
        (HISTORY_START + i * HOUR, 100.0 + i)
        for i in range(48)
        if HISTORY_START + i * HOUR != GAP_TS
    ]
    write_actuals(conn, config, LOAD, history, available_at=ASOF - HOUR)

    horizon_hours = int((HORIZON_END - HISTORY_START) / HOUR)
    # Vintage A: published well before asof, covers history + horizon, value 10.
    write_predictors(
        conn, config, TEMP,
        [(HISTORY_START + i * HOUR, 10.0) for i in range(horizon_hours)],
        available_at=ASOF - timedelta(hours=12),
    )
    # Vintage B: published AFTER asof, value 99 — must be invisible at asof.
    write_predictors(
        conn, config, TEMP,
        [(HISTORY_START + i * HOUR, 99.0) for i in range(horizon_hours)],
        available_at=LATE_VINTAGE_AT,
    )
    conn.commit()
    return conn, config


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        cur.execute(
            "DELETE FROM forecast.forecasts WHERE run_id IN "
            "(SELECT run_id FROM forecast.runs WHERE run_name LIKE %s)",
            (RUN_NAME + "%",),
        )
        cur.execute("DELETE FROM forecast.runs WHERE run_name LIKE %s", (RUN_NAME + "%",))
        for table in ("actuals", "predictors"):
            cur.execute(
                f"DELETE FROM forecast.{table} WHERE series_id IN "
                "(SELECT series_id FROM forecast.series WHERE name IN (%s, %s))",
                (LOAD, TEMP),
            )
        cur.execute("DELETE FROM forecast.series WHERE name IN (%s, %s)", (LOAD, TEMP))
    conn.commit()


@pytest.fixture(scope="module")
def context_dataset(seeded):
    from forecast_store.integrations.openstef import StoreReader

    reader = StoreReader(DSN)
    return reader.context(
        LOAD,
        {"temperature": TEMP},
        history_start=HISTORY_START,
        asof=ASOF,
        horizon_end=HORIZON_END,
    )


def test_grid_and_gapfill(context_dataset):
    frame = context_dataset.data
    assert context_dataset.sample_interval == HOUR
    assert len(frame) == 72  # 48h history + 24h horizon on the declared grid
    # The missing measurement was gapfilled by locf...
    assert frame.loc[GAP_TS, "load"] == frame.loc[GAP_TS - HOUR, "load"]
    # ...and reported in gap stats.
    assert context_dataset.store_context["gap_stats"]["load"] == 1
    # Target history ends at asof; horizon rows are NaN.
    assert frame["load"].last_valid_index().to_pydatetime() == ASOF - HOUR
    assert frame["load"].isna().sum() == 24


def test_vintage_isolation(seeded, context_dataset):
    """The late vintage (published after asof) must be invisible at asof."""
    frame = context_dataset.data
    assert (frame["temperature"] == 10.0).all()  # never 99

    # Reading as-of a later moment sees the superseding vintage.
    from forecast_store.integrations.openstef import StoreReader

    later = StoreReader(DSN).context(
        LOAD,
        {"temperature": TEMP},
        history_start=HISTORY_START,
        asof=LATE_VINTAGE_AT + HOUR,
        horizon_end=HORIZON_END,
    )
    assert (later.data["temperature"] == 99.0).all()


def test_workflow_on_store_context_completes_audit(seeded, context_dataset):
    from openstef_core.types import LeadTime, Q
    from openstef_models.models.forecasting.constant_quantile_forecaster import (
        ConstantQuantileForecaster,
    )
    from openstef_models.models.forecasting_model import ForecastingModel
    from openstef_models.workflows.custom_forecasting_workflow import (
        CustomForecastingWorkflow,
    )

    from forecast_store.integrations.openstef import ForecastStoreCallback, StoreReader

    conn, _config = seeded
    callback = ForecastStoreCallback(DSN, LOAD, auto_register=False)
    workflow = CustomForecastingWorkflow(
        model=ForecastingModel(
            forecaster=ConstantQuantileForecaster(
                horizons=[LeadTime.from_string("PT24H")], quantiles=[Q(0.5)]
            )
        ),
        model_id="ph_a_ctx_model",
        run_name=RUN_NAME,
        callbacks=[callback],
    )
    # Fit on history only (no NaN horizon), predict on the full context.
    history = StoreReader(DSN).context(
        LOAD, {"temperature": TEMP},
        history_start=HISTORY_START, asof=ASOF, horizon_end=ASOF,
    )
    workflow.fit(history)
    result = workflow.predict(context_dataset, forecast_start=ASOF)
    assert len(result.data) > 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT context_end, params FROM forecast.runs WHERE run_id = %s",
            (callback.last_run_id,),
        )
        context_end, params = cur.fetchone()
    # Both halves of the audit: measured history bound + covariate knowledge bound.
    assert context_end == ASOF - HOUR
    assert params["covariates_asof"] == ASOF.isoformat()
    assert params["context_provenance"]["sources"] == {
        "load": {"series": LOAD, "table": "actuals"},
        "temperature": {"series": TEMP, "table": "predictors"},
    }
    assert params["context_provenance"]["gap_stats"]["load"] == 1


def test_target_knowledge_cutoff(seeded):
    """A target revision claimed after asof is invisible at asof (§9.2:
    the knowledge cutoff applies to the target read too, not just covariates).
    Runs last: it appends a revision to the shared seed data."""
    from forecast_store.config import StoreConfig
    from forecast_store.integrations.openstef import StoreReader
    from forecast_store.series import get_series_id
    from forecast_store.write import write_actuals

    conn, config = seeded
    revised_ts = HISTORY_START + 10 * HOUR
    write_actuals(
        conn, config,
        get_series_id(conn, config, LOAD),  # ids stay accepted alongside names
        [(revised_ts, 555.0)],
        available_at=LATE_VINTAGE_AT,  # settlement revision, published after asof
    )
    conn.commit()

    at_asof = StoreReader(DSN).context(
        LOAD, {}, history_start=HISTORY_START, asof=ASOF, horizon_end=ASOF,
    )
    assert at_asof.data.loc[revised_ts, "load"] == 110.0  # original belief

    later = StoreReader(DSN).context(
        LOAD, {}, history_start=HISTORY_START, asof=LATE_VINTAGE_AT + HOUR,
        horizon_end=LATE_VINTAGE_AT + HOUR,
    )
    assert later.data.loc[revised_ts, "load"] == 555.0  # revision visible later


def test_store_binding_schema_conflict():
    """An explicit declaration plus a contradicting ``schema`` is a caller bug,
    refused at construction — before any connection is opened."""
    from forecast_store.config import StoreConfig
    from forecast_store.integrations.openstef import ForecastStoreCallback, StoreReader

    with pytest.raises(ValueError, match="conflicts"):
        StoreReader(DSN, StoreConfig(schema="fs_a"), schema="fs_b")
    with pytest.raises(ValueError, match="conflicts"):
        ForecastStoreCallback(DSN, LOAD, store_config=StoreConfig(schema="fs_a"), schema="fs_b")
    # Agreeing spellings are fine, and an explicit config is never re-read.
    assert StoreReader(DSN, StoreConfig(schema="fs_a"), schema="fs_a")._schema == "fs_a"


def test_forecast_feed_as_covariate(seeded):
    """Our own forecasts consumed as another model's input (spec §6.2): read
    from the forecast log, as-of vintage selection, optional producer pin."""
    from forecast_store.integrations.openstef import ForecastFeed, StoreReader
    from forecast_store.write import write_forecast

    conn, config = seeded
    horizon_ts = [ASOF + i * HOUR for i in range(24)]

    def run(value, available_at, run_name=RUN_NAME):
        return write_forecast(
            conn, config,
            series=LOAD, model="constant", run_name=run_name,
            available_at=available_at,
            points=[(ts, {"q50": value}) for ts in horizon_ts],
        )

    run(42.0, ASOF - timedelta(hours=6))
    run(43.0, ASOF - timedelta(hours=3))          # superseding vintage
    run(99.0, ASOF - HOUR, run_name=RUN_NAME + "_other")  # another producer
    conn.commit()

    reader = StoreReader(DSN)

    # Producer pinned: latest vintage of OUR job wins (43), not the other producer.
    pinned = reader.context(
        LOAD, {"site_fc": ForecastFeed(LOAD, "q50", run_name=RUN_NAME)},
        history_start=HISTORY_START, asof=ASOF, horizon_end=HORIZON_END,
    )
    horizon_rows = pinned.data.loc[ASOF:, "site_fc"]
    assert (horizon_rows == 43.0).all()
    assert pinned.store_context["sources"]["site_fc"]["table"] == "forecasts"

    # Unpinned: latest vintage across all producers (99).
    unpinned = reader.context(
        LOAD, {"site_fc": ForecastFeed(LOAD, "q50")},
        history_start=HISTORY_START, asof=ASOF, horizon_end=HORIZON_END,
    )
    assert (unpinned.data.loc[ASOF:, "site_fc"] == 99.0).all()

    # As-of before the second vintage: the first one (42) is the belief.
    earlier = reader.context(
        LOAD, {"site_fc": ForecastFeed(LOAD, "q50", run_name=RUN_NAME)},
        history_start=HISTORY_START, asof=ASOF - timedelta(hours=4),
        horizon_end=HORIZON_END,
    )
    assert (earlier.data.loc[ASOF:, "site_fc"] == 42.0).all()
