"""Phase B live test: openstef-beam BenchmarkPipeline end-to-end on the store.

Seeds a synthetic target into the store, runs the real BenchmarkPipeline
(median-model preset) twice — once with InMemoryBenchmarkStorage as the
fidelity control, once with TimescaleBenchmarkStorage — through the same
TimescaleTargetProvider, and verifies: simulated knowledge time landed in the
forecast log, evaluation metrics round-trip exactly, re-derived subsets match
the originals, and the pipeline's resume checks short-circuit.
"""

import math
import os
from datetime import datetime, timedelta, timezone

import pytest

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

pytestmark = pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")

LOAD, WEATHER = "bm_load", "bm_weather"  # weather series per column: bm_weather_<col>
RUN_NAME = "bm_test"
HOUR = timedelta(hours=1)
T0 = datetime(2025, 3, 1, tzinfo=timezone.utc)
BENCH_START = T0 + timedelta(days=21)
BENCH_END = T0 + timedelta(days=25)
DATA_END = BENCH_END + timedelta(days=2)
WEATHER_COLS = ("temperature", "radiation", "windspeed")


def _weather_series(col):
    return f"{WEATHER}_{col}"


@pytest.fixture(scope="module")
def seeded(request):
    psycopg = pytest.importorskip("psycopg")
    pytest.importorskip("openstef_beam")

    from forecast_store.config import StoreConfig
    from forecast_store.provision import provision
    from forecast_store.write import write_predictors

    config = StoreConfig()
    provision(DSN, config)
    conn = psycopg.connect(DSN)
    request.addfinalizer(conn.close)
    _cleanup(conn)
    request.addfinalizer(lambda: _cleanup(conn))

    with conn.cursor() as cur:
        cur.execute("SELECT forecast.register_series(%s, interval '1 hour')", (LOAD,))
        load_id = cur.fetchone()[0]
        weather_ids = {}
        for col in WEATHER_COLS:
            cur.execute(
                "SELECT forecast.register_series(%s, interval '1 hour')",
                (_weather_series(col),),
            )
            weather_ids[col] = cur.fetchone()[0]

        # Measurements: deterministic daily/weekly pattern, claims at target time
        # (backfill with per-row availability so backtest cutoffs see history).
        hours = int((DATA_END - T0) / HOUR)
        cur.executemany(
            "INSERT INTO forecast.actuals (series_id, target_time, available_at, value) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            [
                (
                    load_id,
                    T0 + i * HOUR,
                    T0 + i * HOUR,
                    100
                    + 20 * math.sin(2 * math.pi * (i % 24) / 24)
                    + 5 * math.sin(2 * math.pi * (i % 168) / 168),
                )
                for i in range(hours)
            ],
        )

    # Predictors: one early vintage covering the whole span (published at T0).
    for col in WEATHER_COLS:
        write_predictors(
            conn,
            config,
            weather_ids[col],
            [(T0 + i * HOUR, T0, 10.0 + (i % 24)) for i in range(hours)],
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
        cur.execute(
            "DELETE FROM forecast.evaluation_metrics WHERE eval_run_id IN "
            "(SELECT eval_run_id FROM forecast.evaluation_runs WHERE run_name LIKE %s)",
            (RUN_NAME + "%",),
        )
        cur.execute(
            "DELETE FROM forecast.evaluation_series WHERE run_name LIKE %s", (RUN_NAME + "%",)
        )
        cur.execute(
            "DELETE FROM forecast.evaluation_runs WHERE run_name LIKE %s", (RUN_NAME + "%",)
        )
        names = [LOAD] + [_weather_series(c) for c in WEATHER_COLS]
        cur.execute(
            "DELETE FROM forecast.actuals WHERE series_id IN "
            "(SELECT series_id FROM forecast.series WHERE name = ANY(%s))",
            (names,),
        )
        cur.execute(
            "DELETE FROM forecast.predictors WHERE series_id IN "
            "(SELECT series_id FROM forecast.series WHERE name = ANY(%s))",
            (names,),
        )
        cur.execute("DELETE FROM forecast.series WHERE name = ANY(%s)", (names,))
    conn.commit()


def _make_target():
    from openstef_beam.benchmarking.models import BenchmarkTarget

    return BenchmarkTarget(
        name="bm1",
        description="synthetic phase-b target",
        group_name="test",
        latitude=52.1,
        longitude=5.3,
        limit=150.0,
        train_start=T0,
        benchmark_start=BENCH_START,
        benchmark_end=BENCH_END,
    )


def _make_provider():
    from openstef_beam.evaluation.metric_providers import RMAEProvider
    from openstef_core.types import Q

    from forecast_store.integrations.openstef_beam import TimescaleTargetProvider

    return TimescaleTargetProvider(
        dsn=DSN,
        targets=[_make_target()],
        measurement_series={"bm1": LOAD},
        predictor_series={"bm1": {c: _weather_series(c) for c in WEATHER_COLS}},
        metric_providers=[
            RMAEProvider(quantiles=[Q(0.5)], lower_quantile=Q(0.1), upper_quantile=Q(0.9))
        ],
    )


def _make_pipeline(storage, provider):
    from openstef_beam.analysis import AnalysisConfig
    from openstef_beam.backtesting import BacktestConfig
    from openstef_beam.benchmarking.benchmark_pipeline import BenchmarkPipeline
    from openstef_beam.benchmarking.callbacks.strict_execution_callback import (
        StrictExecutionCallback,
    )
    from openstef_beam.evaluation import EvaluationConfig
    from openstef_beam.evaluation.models import Window
    from openstef_core.types import AvailableAt

    return BenchmarkPipeline(
        callbacks=[StrictExecutionCallback()],
        backtest_config=BacktestConfig(
            prediction_sample_interval=HOUR,
            predict_interval=timedelta(hours=24),
            train_interval=timedelta(days=7),
        ),
        evaluation_config=EvaluationConfig(
            available_ats=[AvailableAt.from_string("D-1T06:00")],
            lead_times=[],
            windows=[Window(lag=timedelta(0), size=timedelta(days=3))],
        ),
        analysis_config=AnalysisConfig(visualization_providers=[]),
        target_provider=provider,
        storage=storage,
    )


def _make_factory(tmp_dir):
    from openstef_beam.backtesting.backtest_forecaster import BacktestForecasterConfig
    from openstef_beam.benchmarking.baselines.openstef4 import (
        create_openstef4_preset_backtest_forecaster,
    )
    from openstef_core.types import LeadTime, Q
    from openstef_models.presets.forecasting_workflow import ForecastingWorkflowConfig

    workflow_config = ForecastingWorkflowConfig(
        model_id="bm1",
        model="constant_quantile",
        quantiles=[Q(0.1), Q(0.5), Q(0.9)],
        sample_interval=HOUR,
        horizons=[LeadTime.from_string("PT24H")],
        mlflow_storage=None,  # benchmark artifacts belong to the store, not MLflow
    )
    backtest_config = BacktestForecasterConfig(
        requires_training=True,
        predict_length=timedelta(hours=24),
        predict_min_length=HOUR,
        predict_context_length=timedelta(days=7),
        predict_context_min_coverage=0.5,
        training_context_length=timedelta(days=14),
        training_context_min_coverage=0.5,
        predict_sample_interval=HOUR,
    )
    return create_openstef4_preset_backtest_forecaster(
        workflow_config=workflow_config,
        backtest_config=backtest_config,
        cache_dir=tmp_dir,
    )


@pytest.fixture(scope="module")
def benchmark_results(seeded, tmp_path_factory):
    """Run the benchmark twice: in-memory (control) and store-backed."""
    from openstef_beam.benchmarking.storage.base import InMemoryBenchmarkStorage

    from forecast_store.integrations.openstef_beam import TimescaleBenchmarkStorage

    provider = _make_provider()

    memory = InMemoryBenchmarkStorage()
    _make_pipeline(memory, provider).run(
        forecaster_factory=_make_factory(tmp_path_factory.mktemp("cache_mem")),
        run_name=RUN_NAME,
        n_processes=1,
        skip_analysis=True,
    )

    store = TimescaleBenchmarkStorage(DSN, RUN_NAME, provider)
    _make_pipeline(store, provider).run(
        forecaster_factory=_make_factory(tmp_path_factory.mktemp("cache_db")),
        run_name=RUN_NAME,
        n_processes=1,
        skip_analysis=True,
    )
    return memory, store


def test_backtest_lands_with_simulated_knowledge_time(seeded, benchmark_results):
    conn, _ = seeded
    _, store = benchmark_results
    target = _make_target()
    assert store.has_backtest_output(target)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), min(available_at), max(available_at), "
            "bool_and(recorded_at > available_at) "
            "FROM forecast.runs WHERE run_name = %s",
            (f"{RUN_NAME}/bm1",),
        )
        n_runs, first_event, last_event, all_simulated = cur.fetchone()
    assert n_runs >= 3  # one run per prediction event
    assert first_event >= BENCH_START - timedelta(days=1)
    assert last_event < BENCH_END
    # Simulated vintages: claims in the past, recorded now (spec §4.1/§4.3).
    assert all_simulated


def test_backtest_output_round_trips(benchmark_results):
    """The store persists the declared value columns + knowledge time. Auxiliary
    backtest columns (the target copy, stdev — spec §11 open item) do not
    round-trip: the target rejoins from actuals at evaluation time."""
    import pandas as pd

    memory, store = benchmark_results
    target = _make_target()
    mem_frame = memory.load_backtest_output(target).data
    db_frame = store.load_backtest_output(target).data
    persisted = [c for c in mem_frame.columns if c.startswith("quantile_P")] + ["available_at"]
    db_cmp = db_frame[persisted].sort_index().copy()
    mem_cmp = mem_frame[persisted].sort_index().copy()
    for frame in (db_cmp, mem_cmp):  # pandas datetime unit (ns vs us) is not the contract
        frame["available_at"] = frame["available_at"].astype("datetime64[us, UTC]")
    pd.testing.assert_frame_equal(db_cmp, mem_cmp, check_freq=False)


def test_evaluation_metrics_round_trip_exactly(benchmark_results):
    memory, store = benchmark_results
    target = _make_target()
    assert store.has_evaluation_output(target)

    mem_report = memory.load_evaluation_output(target)
    db_report = store.load_evaluation_output(target)
    assert len(db_report.subset_reports) == len(mem_report.subset_reports) == 1

    mem_subset, db_subset = mem_report.subset_reports[0], db_report.subset_reports[0]
    assert str(db_subset.filtering) == str(mem_subset.filtering)
    # Metrics: lossless pydantic snapshot.
    assert [m.model_dump() for m in db_subset.metrics] == [
        m.model_dump() for m in mem_subset.metrics
    ]
    # Subsets: re-derived from stored backtest output == pipeline's original,
    # on the contracted columns (target + quantiles; stdev is unpersisted, §11).
    import pandas as pd

    contracted = list(db_subset.subset.data.columns)
    assert set(mem_subset.subset.data.columns) - set(contracted) <= {"stdev"}
    pd.testing.assert_frame_equal(
        db_subset.subset.data[contracted],
        mem_subset.subset.data[contracted],
        check_freq=False,
    )


def test_relational_projection_is_queryable(seeded, benchmark_results):
    conn, _ = seeded
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM forecast.evaluation_metrics m "
            "JOIN forecast.evaluation_series s USING (eval_series_id) "
            "WHERE s.run_name = %s AND s.metric = 'rMAE' AND s.quantile = '0.5'",
            (f"{RUN_NAME}/bm1",),
        )
        assert cur.fetchone()[0] > 0


def test_resume_checks_short_circuit(seeded, benchmark_results, tmp_path_factory):
    """Re-running the pipeline must not recompute: has_* short-circuits."""
    conn, _ = seeded
    _, store = benchmark_results
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM forecast.runs WHERE run_name = %s", (f"{RUN_NAME}/bm1",)
        )
        runs_before = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM forecast.evaluation_runs WHERE run_name = %s",
            (f"{RUN_NAME}/bm1",),
        )
        evals_before = cur.fetchone()[0]

    _make_pipeline(store, _make_provider()).run(
        forecaster_factory=_make_factory(tmp_path_factory.mktemp("cache_resume")),
        run_name=RUN_NAME,
        n_processes=1,
        skip_analysis=True,
    )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM forecast.runs WHERE run_name = %s", (f"{RUN_NAME}/bm1",)
        )
        assert cur.fetchone()[0] == runs_before
        cur.execute(
            "SELECT count(*) FROM forecast.evaluation_runs WHERE run_name = %s",
            (f"{RUN_NAME}/bm1",),
        )
        assert cur.fetchone()[0] == evals_before
