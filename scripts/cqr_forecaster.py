"""Split-conformal calibrated variant of the OpenSTEF-4 preset backtest forecaster.

Tests the hypothesis (see docs/benchmark_log.md) that the classical presets'
rCRPS gap is band calibration, not point accuracy: xgboost and gblinear score
nearly identical rCRPS (0.0946 vs 0.0947) despite a 16% rMAE gap, so the
quantile spread — not the median — is the bottleneck.

Uses openstef's own ``ConformalizedQuantileCalibrator`` (upstream PR #1060),
fitted the way split-conformal requires — on *held-out* predictions, never
in-sample (stock postprocessing fits on training-set predictions, which for
gradient boosting understates residuals and under-corrects):

- ``fit()`` trains the preset on the training window minus the trailing
  ``calibration_length``, then replays production-style predictions over the
  held-out window — knowledge cut at each simulated origin
  (``available_before=origin``) and rolled in ``calibration_step`` chunks so
  no lag feature reaches past its origin — and fits the calibrator against
  fit-time actuals (the publication-lagged tail drops out naturally).
- ``predict()`` applies the correction, then re-sorts quantiles (the
  calibrator's asymmetric shifts can cross; it delegates ordering to
  ``QuantileSorter`` by design). The median is left untouched, so rMAE@q50
  should not move — that invariance is part of the experiment.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import override

import pandas as pd
from pydantic import Field, PrivateAttr

from openstef_beam.backtesting.restricted_horizon_timeseries import (
    RestrictedHorizonVersionedTimeSeries,
)
from openstef_beam.benchmarking.baselines.openstef4 import OpenSTEF4BacktestForecaster
from openstef_core.datasets import ForecastDataset, TimeSeriesDataset
from openstef_core.exceptions import FlatlinerDetectedError, InsufficientlyCompleteError
from openstef_models.transforms.postprocessing import (
    ConformalizedQuantileCalibrator,
    QuantileSorter,
)

_MIN_CALIBRATION_PAIRS = 100  # the calibrator's own per-quantile default


class CQRPresetBacktestForecaster(OpenSTEF4BacktestForecaster):
    """OpenSTEF-4 preset + held-out asymmetric conformal quantile calibration."""

    calibration_length: timedelta = Field(default=timedelta(days=14))
    calibration_step: timedelta = Field(default=timedelta(days=3))

    _calibrator: ConformalizedQuantileCalibrator | None = PrivateAttr(default=None)
    _sorter: QuantileSorter = PrivateAttr(default_factory=QuantileSorter)
    _cqr_logger: logging.Logger = PrivateAttr(
        default=logging.getLogger(__name__)
    )

    @override
    def fit(self, data: RestrictedHorizonVersionedTimeSeries) -> None:
        horizon = data.horizon
        cal_start = horizon - self.calibration_length
        workflow = self.workflow_template.with_run_name(horizon.isoformat())
        workflow.callbacks.extend(self.extra_callbacks)

        training_data = data.get_window(
            start=horizon - self.config.training_context_length,
            end=cal_start,  # the held-out tail never reaches training
            available_before=horizon,
        )
        try:
            workflow.fit(data=training_data)
            self._is_flatliner_detected = False
        except FlatlinerDetectedError:
            self._cqr_logger.warning("Flatliner detected during training")
            self._is_flatliner_detected = True
            return
        except InsufficientlyCompleteError:
            self._cqr_logger.warning(
                "Insufficient training data at %s, retaining previous model", horizon
            )
            return

        self._workflow = workflow
        self._calibrator = None
        try:
            self._calibrator = self._fit_calibrator(data, cal_start, horizon)
        except Exception:
            # A raw-quantile forecast beats no forecast; the run label
            # still says what was attempted.
            self._cqr_logger.warning(
                "Conformal calibration failed at %s; raw quantiles this cycle",
                horizon,
                exc_info=True,
            )

    def _fit_calibrator(
        self, data: RestrictedHorizonVersionedTimeSeries, cal_start, horizon
    ) -> ConformalizedQuantileCalibrator:
        assert self._workflow is not None
        target_col = self._workflow.model.target_column

        frames: list[pd.DataFrame] = []
        origin = cal_start
        while origin < horizon:
            end = min(origin + self.calibration_step, horizon)
            window = data.get_window(
                start=origin - self.config.predict_context_length,
                end=end,
                available_before=origin,  # exactly what production would know
            )
            prediction = self._workflow.predict(data=window, forecast_start=origin)
            frames.append(prediction.data)
            origin = end

        predicted = pd.concat(frames).sort_index()
        predicted = predicted[~predicted.index.duplicated(keep="last")]
        quantile_cols = [c for c in predicted.columns if c.startswith("quantile_")]

        # Score against actuals as known at fit time; targets still unpublished
        # (the ~48h feed lag) drop out here rather than counting as errors.
        truth = data.get_window(start=cal_start, end=horizon, available_before=horizon)
        combined = predicted[quantile_cols].copy()
        combined[target_col] = truth.data[target_col].reindex(combined.index)
        combined = combined.dropna(subset=[target_col])
        if len(combined) < _MIN_CALIBRATION_PAIRS:
            raise ValueError(f"only {len(combined)} calibration pairs")

        calibration_set = ForecastDataset(
            data=combined, sample_interval=self.config.predict_sample_interval
        )
        calibrator = ConformalizedQuantileCalibrator()  # all quantiles, median untouched
        calibrator.fit(calibration_set)
        return calibrator

    @override
    def predict(self, data: RestrictedHorizonVersionedTimeSeries) -> TimeSeriesDataset | None:
        forecast = super().predict(data)
        if forecast is None or self._calibrator is None:
            return forecast
        calibrated = self._calibrator.transform(forecast)
        return self._sorter.transform(calibrated)


def create_cqr_preset_backtest_forecaster(workflow_config, cache_dir):  # noqa: ANN001, ANN201
    """Mirror of ``create_openstef4_preset_backtest_forecaster`` (same default
    windows, verbatim) constructing the CQR subclass instead."""
    from pathlib import Path

    from openstef_beam.backtesting.backtest_forecaster.mixins import (
        BacktestForecasterConfig,
    )
    from openstef_models.presets import create_forecasting_workflow
    from openstef_models.presets.forecasting_workflow import LocationConfig
    from pydantic_extra_types.coordinate import Coordinate

    backtest_config = BacktestForecasterConfig(
        requires_training=True,
        predict_length=timedelta(days=7),
        predict_min_length=timedelta(minutes=15),
        predict_context_length=timedelta(days=14),  # Context needed for lag features
        predict_context_min_coverage=0.5,
        training_context_length=timedelta(days=90),  # Three months of training data
        training_context_min_coverage=0.5,
        predict_sample_interval=timedelta(minutes=15),
    )

    def factory(context, target):  # noqa: ANN001
        location = LocationConfig(
            name=target.name,
            description=target.description,
            coordinate=Coordinate(latitude=target.latitude, longitude=target.longitude),
        )
        workflow = create_forecasting_workflow(
            config=workflow_config.model_copy(
                update={"model_id": f"{context.run_name}_{target.name}", "location": location}
            )
        )
        return CQRPresetBacktestForecaster(
            config=backtest_config,
            workflow_template=workflow,
            cache_dir=Path(cache_dir) / f"{context.run_name}_{target.name}",
        )

    return factory
