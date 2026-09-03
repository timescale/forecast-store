"""BacktestForecasterMixin wrappers for additional TSFM benchmark baselines.

TimesFM 2.5 (google, 200M, torch): zero-shot; univariate by default, or
covariate-fed via the package's XReg path (`forecast_with_covariates`): an
in-context ridge regression on the covariates, TimesFM forecasting the
residuals — the transformer stays univariate, covariates enter through a
linear side-channel (unlike Chronos-2/Moirai, which attend over them). The
decile quantile head yields the store-band subset {0.1, 0.3, 0.5, 0.7, 0.9};
q05/q95 are honestly absent, so its rCRPS is computed over a narrower band
than Chronos-2's (disclosed alongside results). The model is loaded and
compiled once and shared across targets, like the official Chronos-2 example.

Publication lag is handled by forecasting *through* the unobserved gap: the
liander target publishes ~48h late, so at decision time S the context ends
~192 steps before S. Forward-filling two days of stale values would bias the
input; instead we forecast (gap + horizon) steps from the last observed point
and keep only the horizon tail.
"""

from __future__ import annotations

from datetime import timedelta
from typing import override

import numpy as np
import pandas as pd

from openstef_beam.backtesting.backtest_forecaster import (
    BacktestForecasterConfig,
    BacktestForecasterMixin,
)
from openstef_beam.backtesting.restricted_horizon_timeseries import (
    RestrictedHorizonVersionedTimeSeries,
)
from openstef_core.datasets import TimeSeriesDataset
from openstef_core.types import Q, Quantile

SAMPLE_INTERVAL = timedelta(minutes=15)
HORIZON_STEPS = 288  # P3D at 15 minutes, matching the other models
CONTEXT_STEPS = 2048  # ~21 days of 15-minute history
MAX_DECODE_STEPS = 512  # horizon + publication-lag gap headroom


def load_timesfm(for_covariates: bool = False):
    """Load and compile TimesFM 2.5 once; the instance is shared per process.

    The XReg path requires ``return_backcast=True`` at compile time; the
    univariate path keeps the plain config.
    """
    import timesfm

    model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(
        timesfm.TimesFM_2p5_200M_torch.DEFAULT_REPO_ID
    )
    model.compile(
        timesfm.ForecastConfig(
            max_context=CONTEXT_STEPS,
            max_horizon=MAX_DECODE_STEPS,
            normalize_inputs=True,
            use_continuous_quantile_head=True,
            fix_quantile_crossing=True,
            return_backcast=for_covariates,
        )
    )
    return model


class TimesFMBacktestForecaster(BacktestForecasterMixin):
    """Zero-shot TimesFM 2.5 under OpenSTEF's backtest/benchmark pipeline.

    ``covariates`` names known-future columns (weather-forecast vintages) fed
    through the XReg side-channel; empty means plain univariate ``forecast()``.
    """

    _LEVELS = (0.1, 0.3, 0.5, 0.7, 0.9)
    _HEAD_INDEX = {0.1: 1, 0.3: 3, 0.5: 5, 0.7: 7, 0.9: 9}  # index 0 is the mean

    def __init__(
        self,
        model,  # noqa: ANN001
        target_column: str = "load",
        covariates: tuple[str, ...] = (),
    ) -> None:
        self._model = model
        self._target = target_column
        self._covariates = covariates
        self._quantiles = [Q(level) for level in self._LEVELS]
        self.config = BacktestForecasterConfig(
            requires_training=False,  # zero-shot
            predict_length=SAMPLE_INTERVAL * HORIZON_STEPS,
            predict_min_length=SAMPLE_INTERVAL,
            predict_context_length=SAMPLE_INTERVAL * CONTEXT_STEPS,
            predict_context_min_coverage=0.3,
            training_context_length=timedelta(0),
            training_context_min_coverage=0.0,
            predict_sample_interval=SAMPLE_INTERVAL,
        )

    @property
    @override
    def quantiles(self) -> list[Quantile]:
        return self._quantiles

    @override
    def fit(self, data: RestrictedHorizonVersionedTimeSeries) -> None:
        return None  # zero-shot

    @override
    def predict(self, data: RestrictedHorizonVersionedTimeSeries) -> TimeSeriesDataset | None:
        window = data.get_window(
            start=data.horizon - self.config.predict_context_length,
            end=data.horizon,
            available_before=data.horizon,
        )
        if self._target not in window.data.columns:
            return None
        grid = pd.date_range(
            end=data.horizon - SAMPLE_INTERVAL, periods=CONTEXT_STEPS, freq=SAMPLE_INTERVAL
        )
        history = window.data[self._target].reindex(grid).astype(np.float32)

        last_valid = history.last_valid_index()
        if last_valid is None:
            return None
        # Steps between the last observed point and the decision time: the
        # publication-lag gap the model must forecast through.
        gap_steps = int((data.horizon - last_valid) / SAMPLE_INTERVAL) - 1
        decode_steps = gap_steps + HORIZON_STEPS
        if decode_steps > MAX_DECODE_STEPS:
            return None  # context too stale to reach the horizon honestly

        context = history.loc[:last_valid].ffill().dropna()
        if len(context) < 96:  # require at least a day of observed history
            return None

        if self._covariates:
            # Covariate sequences span context + decode window; the split into
            # train/test halves inside forecast_with_covariates is by length.
            future = data.get_window(
                start=data.horizon,
                end=data.horizon + SAMPLE_INTERVAL * HORIZON_STEPS,
                available_before=data.horizon,
            )
            cov_grid = pd.date_range(
                start=context.index[0],
                periods=len(context) + decode_steps,
                freq=SAMPLE_INTERVAL,
            )
            both = pd.concat([window.data, future.data]).sort_index()
            both = both[~both.index.duplicated(keep="last")]
            dynamic = {
                col: [
                    both[col].reindex(cov_grid).ffill().bfill().fillna(0.0)
                    .to_numpy(np.float32)
                    if col in both.columns else np.zeros(len(cov_grid), np.float32)
                ]
                for col in self._covariates
            }
            _point, quantile_out = self._model.forecast_with_covariates(
                inputs=[context.to_numpy()],
                dynamic_numerical_covariates=dynamic,
                xreg_mode="xreg + timesfm",
            )
        else:
            _point, quantile_out = self._model.forecast(
                horizon=decode_steps, inputs=[context.to_numpy()]
            )
        q_out = np.asarray(quantile_out[0])  # (decode_steps, 10)
        tail = slice(gap_steps, gap_steps + HORIZON_STEPS)
        index = pd.DatetimeIndex(
            pd.date_range(data.horizon, periods=HORIZON_STEPS, freq=SAMPLE_INTERVAL),
            name="datetime",
        )
        frame = {
            q.format(): q_out[tail, self._HEAD_INDEX[float(q)]]
            for q in self._quantiles
        }
        frame[self._target] = q_out[tail, self._HEAD_INDEX[0.5]]
        return TimeSeriesDataset(
            data=pd.DataFrame(frame, index=index),
            sample_interval=SAMPLE_INTERVAL,
        )


def load_moirai(n_covariates: int = 3):
    """Load Moirai 2.0 R small once; quantile-native, covariate-capable.

    Installed from uni2ts PR #256's branch (main pins numpy~=1.26/torch<2.5,
    incompatible with openstef-core) — benchmark-baseline risk tolerance only.
    """
    from uni2ts.model.moirai2 import Moirai2Forecast, Moirai2Module

    module = Moirai2Module.from_pretrained("Salesforce/moirai-2.0-R-small")
    forecast = Moirai2Forecast(
        module=module,
        prediction_length=MAX_DECODE_STEPS,
        context_length=1024,
        target_dim=1,
        feat_dynamic_real_dim=n_covariates,
        past_feat_dynamic_real_dim=0,
    )
    return forecast.create_predictor(batch_size=1)


class MoiraiBacktestForecaster(BacktestForecasterMixin):
    """Zero-shot Moirai 2.0 with known-future weather covariates.

    Native decile head -> the same store-band subset as TimesFM
    ({0.1, 0.3, 0.5, 0.7, 0.9}); tail levels would be gluonts extrapolation,
    not model output, so they are not emitted. Publication lag handled by
    forecasting through the gap, as in :class:`TimesFMBacktestForecaster`.
    """

    _LEVELS = (0.1, 0.3, 0.5, 0.7, 0.9)
    _COVARIATES = ("shortwave_radiation", "wind_speed_80m", "temperature_2m")
    _CONTEXT_STEPS = 1024

    def __init__(
        self,
        predictor,  # noqa: ANN001
        target_column: str = "load",
        covariates: tuple[str, ...] | None = None,
    ) -> None:
        self._predictor = predictor
        self._target = target_column
        # Must match the feat_dynamic_real_dim the predictor was built with.
        self._covariates = self._COVARIATES if covariates is None else covariates
        self._quantiles = [Q(level) for level in self._LEVELS]
        self.config = BacktestForecasterConfig(
            requires_training=False,
            predict_length=SAMPLE_INTERVAL * HORIZON_STEPS,
            predict_min_length=SAMPLE_INTERVAL,
            predict_context_length=SAMPLE_INTERVAL * self._CONTEXT_STEPS,
            predict_context_min_coverage=0.3,
            training_context_length=timedelta(0),
            training_context_min_coverage=0.0,
            predict_sample_interval=SAMPLE_INTERVAL,
        )

    @property
    @override
    def quantiles(self) -> list[Quantile]:
        return self._quantiles

    @override
    def fit(self, data: RestrictedHorizonVersionedTimeSeries) -> None:
        return None  # zero-shot

    @override
    def predict(self, data: RestrictedHorizonVersionedTimeSeries) -> TimeSeriesDataset | None:
        import pandas as pd
        from gluonts.dataset.common import ListDataset

        past = data.get_window(
            start=data.horizon - self.config.predict_context_length,
            end=data.horizon,
            available_before=data.horizon,
        )
        future = data.get_window(
            start=data.horizon,
            end=data.horizon + SAMPLE_INTERVAL * HORIZON_STEPS,
            available_before=data.horizon,
        )
        if self._target not in past.data.columns:
            return None

        ctx_grid = pd.date_range(
            end=data.horizon - SAMPLE_INTERVAL,
            periods=self._CONTEXT_STEPS,
            freq=SAMPLE_INTERVAL,
        )
        target_hist = past.data[self._target].reindex(ctx_grid).astype(np.float32)
        last_valid = target_hist.last_valid_index()
        if last_valid is None:
            return None
        gap_steps = int((data.horizon - last_valid) / SAMPLE_INTERVAL) - 1
        if gap_steps + HORIZON_STEPS > MAX_DECODE_STEPS:
            return None
        context = target_hist.loc[:last_valid].ffill().dropna()
        if len(context) < 96:
            return None

        # Known-future covariates must span context + prediction window.
        cov_grid = pd.date_range(
            start=context.index[0],
            periods=len(context) + MAX_DECODE_STEPS,
            freq=SAMPLE_INTERVAL,
        )
        both = pd.concat([past.data, future.data]).sort_index()
        both = both[~both.index.duplicated(keep="last")]
        covariates = np.stack([
            both[col].reindex(cov_grid).ffill().bfill().fillna(0.0).to_numpy(np.float32)
            if col in both.columns else np.zeros(len(cov_grid), np.float32)
            for col in self._covariates
        ])

        entry = {
            "start": pd.Period(context.index[0], freq="15min"),
            "target": context.to_numpy(),
            "feat_dynamic_real": covariates,
        }
        forecast = next(iter(self._predictor.predict(ListDataset([entry], freq="15min"))))
        tail = slice(gap_steps, gap_steps + HORIZON_STEPS)
        index = pd.DatetimeIndex(
            pd.date_range(data.horizon, periods=HORIZON_STEPS, freq=SAMPLE_INTERVAL),
            name="datetime",
        )
        frame = {q.format(): forecast.quantile(float(q))[tail] for q in self._quantiles}
        frame[self._target] = forecast.quantile(0.5)[tail]
        return TimeSeriesDataset(
            data=pd.DataFrame(frame, index=index),
            sample_interval=SAMPLE_INTERVAL,
        )


def load_timesfm3(batch_size: int = 1):
    """Load TimesFM 3.0 (330M) once; shared per process.

    Natively multivariate: known-future covariates go in as
    ``past_future_covariates`` — attended by the model, not a linear
    side-channel like 2.5's XReg. Weights are under the TimesFM
    Non-Commercial License v1.0 (benchmark/evaluation use only, never
    production).
    """
    from timesfm3 import ModelConfig, TimesFM3Evaluator

    return TimesFM3Evaluator(ModelConfig(per_core_batch_size=batch_size, device="cpu"))


class TimesFM3BacktestForecaster(BacktestForecasterMixin):
    """Zero-shot TimesFM 3.0 under OpenSTEF's backtest/benchmark pipeline.

    Same decile head as 2.5 (9 levels, median at index 4) -> the same
    store-band subset {0.1, 0.3, 0.5, 0.7, 0.9} and the same
    forecast-through-the-gap handling. ``covariates`` names known-future
    columns fed natively; empty means univariate.
    """

    _LEVELS = (0.1, 0.3, 0.5, 0.7, 0.9)
    _Q_INDEX = {0.1: 0, 0.3: 2, 0.5: 4, 0.7: 6, 0.9: 8}  # into the 9-level head

    def __init__(
        self,
        evaluator,  # noqa: ANN001
        target_column: str = "load",
        covariates: tuple[str, ...] = (),
    ) -> None:
        self._evaluator = evaluator
        self._target = target_column
        self._covariates = covariates
        self._quantiles = [Q(level) for level in self._LEVELS]
        self.config = BacktestForecasterConfig(
            requires_training=False,  # zero-shot
            predict_length=SAMPLE_INTERVAL * HORIZON_STEPS,
            predict_min_length=SAMPLE_INTERVAL,
            predict_context_length=SAMPLE_INTERVAL * CONTEXT_STEPS,
            predict_context_min_coverage=0.3,
            training_context_length=timedelta(0),
            training_context_min_coverage=0.0,
            predict_sample_interval=SAMPLE_INTERVAL,
        )

    @property
    @override
    def quantiles(self) -> list[Quantile]:
        return self._quantiles

    @override
    def fit(self, data: RestrictedHorizonVersionedTimeSeries) -> None:
        return None  # zero-shot

    @override
    def predict(self, data: RestrictedHorizonVersionedTimeSeries) -> TimeSeriesDataset | None:
        window = data.get_window(
            start=data.horizon - self.config.predict_context_length,
            end=data.horizon,
            available_before=data.horizon,
        )
        if self._target not in window.data.columns:
            return None
        grid = pd.date_range(
            end=data.horizon - SAMPLE_INTERVAL, periods=CONTEXT_STEPS, freq=SAMPLE_INTERVAL
        )
        history = window.data[self._target].reindex(grid).astype(np.float32)
        last_valid = history.last_valid_index()
        if last_valid is None:
            return None
        gap_steps = int((data.horizon - last_valid) / SAMPLE_INTERVAL) - 1
        decode_steps = gap_steps + HORIZON_STEPS
        if decode_steps > MAX_DECODE_STEPS:
            return None  # context too stale to reach the horizon honestly

        context = history.loc[:last_valid].ffill().dropna()
        if len(context) < 96:
            return None

        past_future = None
        if self._covariates:
            future = data.get_window(
                start=data.horizon,
                end=data.horizon + SAMPLE_INTERVAL * HORIZON_STEPS,
                available_before=data.horizon,
            )
            cov_grid = pd.date_range(
                start=context.index[0],
                periods=len(context) + decode_steps,
                freq=SAMPLE_INTERVAL,
            )
            both = pd.concat([window.data, future.data]).sort_index()
            both = both[~both.index.duplicated(keep="last")]
            cov = np.stack([
                both[col].reindex(cov_grid).ffill().bfill().fillna(0.0)
                .to_numpy(np.float32)
                if col in both.columns else np.zeros(len(cov_grid), np.float32)
                for col in self._covariates
            ])
            past_future = [cov]

        out = next(iter(self._evaluator.predict_batch(
            contexts=[context.to_numpy()],
            horizon=decode_steps,
            past_future_covariates=past_future,
            return_quantiles=True,
            make_positive=False,  # the liander convention: production is negative
        )))
        q_out = np.asarray(out.quantiles)  # (decode_steps, 9)
        tail = slice(gap_steps, gap_steps + HORIZON_STEPS)
        index = pd.DatetimeIndex(
            pd.date_range(data.horizon, periods=HORIZON_STEPS, freq=SAMPLE_INTERVAL),
            name="datetime",
        )
        frame = {
            q.format(): q_out[tail, self._Q_INDEX[float(q)]]
            for q in self._quantiles
        }
        frame[self._target] = q_out[tail, self._Q_INDEX[0.5]]
        return TimeSeriesDataset(
            data=pd.DataFrame(frame, index=index),
            sample_interval=SAMPLE_INTERVAL,
        )
