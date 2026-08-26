"""Run the liander2024 Chronos-2 benchmark against a forecast store.

Same models, configs, and harness as OpenSTEF's own Chronos-2 benchmark
example — with the store as the only data source (TimescaleTargetProvider)
and the only result sink (TimescaleBenchmarkStorage). Requires the target to
be ingested first (scripts/ingest_liander.py).

    uv run --extra openstef --extra foundation python scripts/run_liander_chronos2.py \
        [--group wind_park] [--target "<name>"] [--start 2024-06-01] [--days 7] [--dsn ...]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "1")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from datetime import datetime, timedelta, timezone  # noqa: E402

from ingest_liander import (  # noqa: E402  (also loads .env)
    CHRONOS_FEATURES,
    load_target_entry,
    series_names,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="wind_park")
    parser.add_argument("--target", default=None)
    parser.add_argument("--start", default="2024-06-01")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--run-name", default="liander_chronos2")
    parser.add_argument("--dsn", default=os.environ.get("FORECAST_STORE_TEST_DSN"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s] %(message)s")

    import psycopg
    from openstef_beam.analysis import AnalysisConfig
    from openstef_beam.backtesting import BacktestConfig
    from openstef_beam.benchmarking.benchmark_pipeline import BenchmarkPipeline
    from openstef_beam.benchmarking.callbacks.strict_execution_callback import (
        StrictExecutionCallback,
    )
    from openstef_beam.benchmarking.models.benchmark_target import BenchmarkTarget
    from openstef_beam.evaluation import EvaluationConfig
    from openstef_beam.evaluation.metric_providers import RCRPSProvider, RMAEProvider
    from openstef_beam.evaluation.models import Window
    from openstef_core.types import AvailableAt, LeadTime, Q
    from openstef_foundation_models.integrations.beam import FoundationModelBacktestForecaster
    from openstef_foundation_models.models import CheckpointVariant, Chronos2
    from openstef_foundation_models.presets.forecasting_workflow import (
        ForecastingWorkflowConfig,
        create_forecasting_workflow,
    )
    from openstef_models.utils.feature_selection import Include

    from forecast_store.integrations.openstef_beam import (
        TimescaleBenchmarkStorage,
        TimescaleTargetProvider,
    )

    entry = load_target_entry(args.group, args.target)
    load_series, all_weather = series_names(args.group, entry["name"])
    # The official Chronos-2 example's three covariates, not the full ingest set.
    weather_series = {col: all_weather[col] for col in CHRONOS_FEATURES}

    bench_start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    target = BenchmarkTarget(
        name=entry["name"],
        description=entry["description"],
        group_name=entry["group_name"],
        latitude=entry["latitude"],
        longitude=entry["longitude"],
        upper_limit=entry["upper_limit"],
        lower_limit=entry["lower_limit"],
        train_start=bench_start - timedelta(days=30),  # zero-shot context window
        benchmark_start=bench_start,
        benchmark_end=bench_start + timedelta(days=args.days),
    )

    provider = TimescaleTargetProvider(
        dsn=args.dsn,
        targets=[target],
        measurement_series={target.name: load_series},
        predictor_series={target.name: weather_series},
        metric_providers=[
            RMAEProvider(quantiles=[Q(0.5)], lower_quantile=Q(0.01), upper_quantile=Q(0.99)),
            RCRPSProvider(lower_quantile=Q(0.01), upper_quantile=Q(0.99)),
        ],
        data_margin=timedelta(days=4),  # 3-day horizon tail + slack
    )

    # Identical model wiring to OpenSTEF's own Chronos-2 benchmark example.
    workflow = create_forecasting_workflow(
        ForecastingWorkflowConfig(
            model="chronos2",
            checkpoint=Chronos2.BASE.checkpoint(CheckpointVariant.recommended()),
            quantiles=[Q(0.05), Q(0.1), Q(0.3), Q(0.5), Q(0.7), Q(0.9), Q(0.95)],
            horizons=[LeadTime.from_string("P3D")],
            target_column="load",
            selected_features=Include("load", *CHRONOS_FEATURES),
        )
    )

    storage = TimescaleBenchmarkStorage(
        args.dsn, args.run_name, provider, model_label="chronos2-base"
    )

    BenchmarkPipeline(
        backtest_config=BacktestConfig(
            prediction_sample_interval=timedelta(minutes=15),
            predict_interval=timedelta(hours=6),
            train_interval=timedelta(days=7),
        ),
        evaluation_config=EvaluationConfig(
            available_ats=[AvailableAt.from_string("D-1T06:00")],
            lead_times=[],
            windows=[Window(lag=timedelta(0), size=timedelta(days=7))],
        ),
        analysis_config=AnalysisConfig(visualization_providers=[]),
        target_provider=provider,
        storage=storage,
        callbacks=[StrictExecutionCallback()],
    ).run(
        forecaster_factory=lambda _ctx, _t: FoundationModelBacktestForecaster.from_workflow(
            workflow, batch_size=1
        ),
        run_name=args.run_name,
        n_processes=1,
        skip_analysis=True,
    )

    # Summary straight from the store: accuracy as queryable rows (spec §2).
    label = f"{args.run_name}/{target.name}"
    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM forecast.runs WHERE run_name = %s", (label,))
        n_runs = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM forecast.forecasts f JOIN forecast.runs r "
            "ON r.run_id = f.run_id WHERE r.run_name = %s",
            (label,),
        )
        n_points = cur.fetchone()[0]
        cur.execute(
            "SELECT s.metric, s.quantile, m.value FROM forecast.evaluation_metrics m "
            "JOIN forecast.evaluation_series s USING (eval_series_id) "
            "WHERE s.run_name = %s AND s.win = 'global' ORDER BY s.metric, s.quantile",
            (label,),
        )
        metrics = cur.fetchall()

    print(f"\n=== {label} ===")
    print(f"forecast runs (simulated vintages): {n_runs}")
    print(f"forecast points: {n_points}")
    for metric, quantile, value in metrics:
        print(f"  {metric:>24} @ {quantile:>7}: {value:.4f}" if value is not None else f"  {metric} @ {quantile}: NULL")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
