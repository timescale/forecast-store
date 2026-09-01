"""Run liander2024 benchmarks against a forecast store — multi-model, multi-target.

Models (each mirroring OpenSTEF's own benchmark example for it exactly):

- ``chronos2-base`` / ``chronos2-small`` — zero-shot TSFMs (ONNX; batched
  inference, single process; the official example's three weather covariates)
- ``xgboost`` / ``gblinear`` — OpenSTEF 4 preset models, trained in-backtest
  (weekly retrain; the full covariate set; parallel across targets)
- ``timesfm`` (univariate) / ``timesfm-cov`` (3 covariates via XReg) /
  ``timesfm-allwx`` (all 11) — zero-shot, decile band
- ``moirai`` (3 covariates) / ``moirai-allwx`` (all 11) — zero-shot, decile band
- ``chronos2-base-allwx`` — the base checkpoint fed all 11 weather covariates
  (deviates from the official example's 3; isolates covariate access)

The store is the only data source (TimescaleTargetProvider) and the only
result sink (TimescaleBenchmarkStorage). Targets must be ingested first
(scripts/ingest_liander.py --all-targets).

    uv run --extra openstef --extra foundation python scripts/run_liander_benchmark.py \
        [--models chronos2-base,chronos2-small,xgboost,gblinear] \
        [--group wind_park] [--targets all] \
        [--start 2024-03-01] [--days 306] [--run-prefix liander] [--dsn ...]

Defaults run the official benchmark window (2024-03-01 + 306 days). Run labels
are ``{run-prefix}_{model}/{target.name}``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from datetime import datetime, timedelta, timezone  # noqa: E402

from ingest_liander import (  # noqa: E402  (also loads .env)
    CHRONOS_FEATURES,
    WEATHER_COLUMNS,
    load_group_entries,
    series_names,
)

MODELS = (
    "chronos2-base",
    "chronos2-small",
    "xgboost",
    "gblinear",
    "timesfm",
    "moirai",
    # Covariate-access variants: -allwx feeds every weather column the store
    # holds instead of the official example's three; timesfm-cov feeds the
    # three through the XReg side-channel (the checkpoint itself is univariate).
    "chronos2-base-allwx",
    "timesfm-cov",
    "timesfm-allwx",
    "moirai-allwx",
    # Band-calibration variant: the preset + openstef's split-conformal
    # quantile calibrator fitted on a held-out window (see cqr_forecaster.py).
    "xgboost-cqr",
)
CHRONOS_BATCH_SIZE = 16  # the official example's batched-inference setting

# timesfm/moirai emit native deciles only; they write to a forecast-log
# instance whose declared band is true (spec §7.2: a different band is the
# canonical reason to split instances), not to the 7-level canonical table.
DECILE_TABLE = "forecasts_deciles"
DECILE_BAND = ("0.1", "0.3", "0.5", "0.7", "0.9")
DECILE_MODELS = frozenset(
    {"timesfm", "timesfm-cov", "timesfm-allwx", "moirai", "moirai-allwx"}
)


def build_forecaster_factory(model: str, cache_dir: Path, quantiles, horizons):
    """(forecaster_factory, n_processes) for one model, per its official example."""
    if model.startswith("chronos2"):
        from openstef_foundation_models.integrations.beam import (
            FoundationModelBacktestForecaster,
        )
        from openstef_foundation_models.models import CheckpointVariant, Chronos2
        from openstef_foundation_models.presets.forecasting_workflow import (
            ForecastingWorkflowConfig,
            create_forecasting_workflow,
        )
        from openstef_models.utils.feature_selection import Include

        base_name = model.removesuffix("-allwx")
        features = WEATHER_COLUMNS if model.endswith("-allwx") else CHRONOS_FEATURES
        size = Chronos2.BASE if base_name.endswith("base") else Chronos2.SMALL
        # DYNAMIC, not recommended(): the static-shape variant (macOS/CoreML)
        # freezes the batch dimension and rejects batched backtest inference.
        workflow = create_forecasting_workflow(
            ForecastingWorkflowConfig(
                model="chronos2",
                checkpoint=size.checkpoint(CheckpointVariant.DYNAMIC),
                quantiles=quantiles,
                horizons=horizons,
                target_column="load",
                selected_features=Include("load", *features),
            )
        )

        def factory(_context, _target):  # noqa: ANN001
            return FoundationModelBacktestForecaster.from_workflow(
                workflow, batch_size=CHRONOS_BATCH_SIZE
            )

        # A live ONNX session cannot be shared across worker processes.
        return factory, 1

    if model.startswith("timesfm"):
        # Zero-shot, decile quantile head (band subset 0.1..0.9); requires
        # `--extra timesfm`. One shared compiled model per process. Variants:
        # bare = univariate; -cov = the official 3 covariates via XReg;
        # -allwx = every weather column via XReg.
        from tsfm_forecasters import TimesFMBacktestForecaster, load_timesfm

        covariates = {
            "timesfm": (),
            "timesfm-cov": CHRONOS_FEATURES,
            "timesfm-allwx": WEATHER_COLUMNS,
        }[model]
        shared = load_timesfm(for_covariates=bool(covariates))

        def factory(_context, _target):  # noqa: ANN001
            return TimesFMBacktestForecaster(shared, covariates=covariates)

        return factory, 1

    if model.startswith("moirai"):
        # Zero-shot; decile band subset like timesfm. Requires `--extra moirai`
        # (uni2ts PR-256 branch). Bare = the official three weather covariates;
        # -allwx = every weather column (native any-variate attention).
        from tsfm_forecasters import MoiraiBacktestForecaster, load_moirai

        covariates = WEATHER_COLUMNS if model.endswith("-allwx") else CHRONOS_FEATURES
        shared = load_moirai(n_covariates=len(covariates))

        def factory(_context, _target):  # noqa: ANN001
            return MoiraiBacktestForecaster(shared, covariates=covariates)

        return factory, 1

    from openstef_beam.benchmarking.baselines.openstef4 import (
        create_openstef4_preset_backtest_forecaster,
    )
    from openstef_models.presets import ForecastingWorkflowConfig

    base_model = model.removesuffix("-cqr")
    config = ForecastingWorkflowConfig(
        model_id="common_model_",
        run_name=None,
        model=base_model,
        horizons=horizons,
        quantiles=quantiles,
        model_reuse_enable=True,
        mlflow_storage=None,
        radiation_column="shortwave_radiation",
        rolling_aggregate_features=["mean", "median", "max", "min"],
        wind_speed_column="wind_speed_80m",
        pressure_column="surface_pressure",
        temperature_column="temperature_2m",
        relative_humidity_column="relative_humidity_2m",
        energy_price_column="EPEX_NL",
    )
    if model.endswith("-cqr"):
        # Same preset, wrapped with held-out split-conformal calibration.
        from cqr_forecaster import create_cqr_preset_backtest_forecaster

        factory = create_cqr_preset_backtest_forecaster(
            workflow_config=config, cache_dir=cache_dir
        )
    else:
        factory = create_openstef4_preset_backtest_forecaster(
            workflow_config=config, cache_dir=cache_dir
        )
    n_processes = int(os.environ.get("OPENSTEF_N_PROCESSES", "0")) or min(
        4, os.cpu_count() or 1
    )
    return factory, n_processes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", default=",".join(MODELS))
    parser.add_argument("--group", default="wind_park")
    parser.add_argument("--targets", default="all", help="'all' or comma-separated target names")
    parser.add_argument("--start", default="2024-03-01")
    parser.add_argument("--days", type=int, default=306)
    parser.add_argument("--run-prefix", default="liander")
    parser.add_argument("--dsn", default=os.environ.get("FORECAST_STORE_TEST_DSN"))
    parser.add_argument("--cache-dir", default=None, help="training cache for preset models")
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

    from forecast_store.integrations.openstef_beam import (
        TimescaleBenchmarkStorage,
        TimescaleTargetProvider,
    )

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    unknown = [m for m in models if m not in MODELS]
    if unknown:
        parser.error(f"unknown models {unknown}; choose from {MODELS}")

    entries = load_group_entries(args.group)
    if args.targets != "all":
        wanted = {t.strip() for t in args.targets.split(",")}
        entries = [e for e in entries if e["name"] in wanted]
        missing = wanted - {e["name"] for e in entries}
        if missing:
            parser.error(f"unknown targets: {sorted(missing)}")

    bench_start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    bench_end = bench_start + timedelta(days=args.days)
    quantiles = [Q(0.05), Q(0.1), Q(0.3), Q(0.5), Q(0.7), Q(0.9), Q(0.95)]
    horizons = [LeadTime.from_string("P3D")]
    cache_dir = Path(args.cache_dir) if args.cache_dir else Path(".benchmark_cache")

    targets, measurement_series, predictor_series = [], {}, {}
    for entry in entries:
        load_series, weather_series = series_names(args.group, entry["name"])
        targets.append(
            BenchmarkTarget(
                name=entry["name"],
                description=entry["description"],
                group_name=entry["group_name"],
                latitude=entry["latitude"],
                longitude=entry["longitude"],
                upper_limit=entry["upper_limit"],
                lower_limit=entry["lower_limit"],
                # Official per-target training history start (2024-01-01).
                train_start=datetime.fromisoformat(str(entry["train_start"])),
                benchmark_start=bench_start,
                benchmark_end=bench_end,
            )
        )
        measurement_series[entry["name"]] = load_series
        predictor_series[entry["name"]] = weather_series

    provider = TimescaleTargetProvider(
        dsn=args.dsn,
        targets=targets,
        measurement_series=measurement_series,
        predictor_series=predictor_series,
        metric_providers=[
            RMAEProvider(quantiles=[Q(0.5)], lower_quantile=Q(0.01), upper_quantile=Q(0.99)),
            RCRPSProvider(lower_quantile=Q(0.01), upper_quantile=Q(0.99)),
        ],
        data_margin=timedelta(days=4),  # 3-day horizon tail + slack
    )

    if DECILE_MODELS & set(models):
        from forecast_store.config import ForecastLogSpec, StoreConfig
        from forecast_store.provision import provision

        provision(  # additive: declares the deciles instance if absent
            args.dsn,
            StoreConfig(extra_tables=(
                ForecastLogSpec(DECILE_TABLE, quantile_band=DECILE_BAND, has_mean=False),
            )),
        )

    for model in models:
        label = f"{args.run_prefix}_{model}"
        factory, n_processes = build_forecaster_factory(model, cache_dir, quantiles, horizons)
        print(f"\n##### {label}: {len(targets)} target(s), {args.days} days, "
              f"n_processes={n_processes} #####")
        started = time.monotonic()
        BenchmarkPipeline(
            backtest_config=BacktestConfig(
                prediction_sample_interval=timedelta(minutes=15),
                predict_interval=timedelta(hours=6),
                train_interval=timedelta(days=7),
            ),
            evaluation_config=EvaluationConfig(
                available_ats=[AvailableAt.from_string("D-1T06:00")],
                lead_times=[],
                windows=[
                    Window(lag=timedelta(0), size=timedelta(days=7)),
                    Window(lag=timedelta(0), size=timedelta(days=21)),
                    Window(lag=timedelta(0), size=timedelta(days=30)),
                ],
            ),
            analysis_config=AnalysisConfig(visualization_providers=[]),
            target_provider=provider,
            storage=TimescaleBenchmarkStorage(
                args.dsn, label, provider, model_label=model,
                forecast_table=DECILE_TABLE if model in DECILE_MODELS else "forecasts",
            ),
            callbacks=[StrictExecutionCallback()],
        ).run(
            forecaster_factory=factory,
            run_name=label,
            n_processes=n_processes,
            skip_analysis=True,
        )
        print(f"##### {label} finished in {time.monotonic() - started:.0f}s #####")

    # Summary straight from the store: one row per model x target x metric.
    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.run_name, s.metric, s.quantile, m.value
            FROM forecast.evaluation_metrics m
            JOIN forecast.evaluation_series s USING (eval_series_id)
            WHERE s.run_name LIKE %s AND s.win = 'global'
              AND (s.metric = 'rCRPS' OR (s.metric = 'rMAE' AND s.quantile = '0.5'))
            ORDER BY s.metric, s.run_name
            """,
            (args.run_prefix + r"\_%",),
        )
        print(f"\n=== global metrics ({args.run_prefix}_*) ===")
        for run_name, metric, quantile, value in cur.fetchall():
            print(f"  {metric:>6} @ {quantile:>6}  {run_name}: {value:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
