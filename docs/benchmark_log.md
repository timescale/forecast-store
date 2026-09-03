# Benchmark log

A running record of every benchmark executed against the forecast store: what
ran, what it scored, where its rows live in the database, and what is planned
next. Numbers here are read back from the store's evaluation tables, never
transcribed from terminal output — the store is the record.

## Common setup (all runs)

- **Dataset**: OpenSTEF's public liander2024 benchmark
  (`OpenSTEF/liander2024-energy-forecasting-benchmark` on Hugging Face);
  `wind_park` group = 5 targets. Real grid measurements with their real
  ~48-hour publication lags; weather as versioned forecast vintages.
- **Window**: the official one — 2024-03-01 + 306 days.
- **Cadence**: a forecast vintage every 6 hours → 1,224 vintages per target
  (6,120 per model); horizon P3D at 15 minutes (288 steps).
- **Evaluation**: OpenSTEF's engine; availability filter `D-1T06:00`,
  windows 7/21/30 days + `global`; metrics rCRPS and rMAE.
- **Store**: sole data source (`TimescaleTargetProvider`) and sole result
  sink (`TimescaleBenchmarkStorage`). Harness:
  `scripts/run_liander_benchmark.py`; ingest first via
  `scripts/ingest_liander.py --all-targets`; DSN from
  `FORECAST_STORE_TEST_DSN`.
- **Headline aggregation**: per-park `win = 'global'` values, averaged
  across the 5 parks (rCRPS at `quantile = 'global'`, rMAE at `0.5`).

## Where everything lives in the store

**Input series** (60 rows in `forecast.series`, all 15-minute grids), named
`ln24/wind_park/<target_slug>/…`:

- `…/load` — the measured target, in `forecast.actuals`
  (1 per park)
- `…/wx/<column>` — weather forecast vintages, in `forecast.predictors`
  (11 per park: `temperature_2m`, `relative_humidity_2m`, `surface_pressure`,
  `cloud_cover`, `wind_speed_10m`, `wind_speed_80m`, `wind_direction_10m`,
  `shortwave_radiation`, `direct_radiation`, `diffuse_radiation`,
  `direct_normal_irradiance`)

Target slugs: `within_15_kilometers_of_alphen_aan_den_rijn_normalized`,
`within_15_kilometers_of_dronten_normalized`,
`within_15_kilometers_of_opmeer_normalized`,
`within_20_kilometers_of_leeuwarden_normalized`,
`within_stadsregio_arnhem_nijmegen_normalized`.

**Output rows**, labeled `liander_<model>/<target name>`:

- Forecast vintages: `forecast.runs` + points in `forecast.forecasts`
  (7-level band, ~10.5M rows) or `forecast.forecasts_deciles` (decile-band
  instance for timesfm/moirai, ~2.8M rows)
- Results: `forecast.evaluation_runs` / `evaluation_series` /
  `evaluation_metrics`

Reproduce any headline number:

```sql
SELECT s.run_name, s.metric, avg(m.value)
FROM forecast.evaluation_metrics m
JOIN forecast.evaluation_series s USING (eval_series_id)
WHERE s.run_name LIKE 'liander\_%' AND s.win = 'global'
  AND (s.metric = 'rCRPS' OR (s.metric = 'rMAE' AND s.quantile = '0.5'))
GROUP BY 1, 2 ORDER BY 1, 2;
```

## Completed benchmarks

All models score identical store-served data. Parks abbreviated: Alphen,
Dronten, Opmeer, Leeuwarden, Arnhem.

### Headline (avg of 5 parks, global window)

| model | run label prefix | covariates | band | avg rCRPS | avg rMAE@q50 | completed (UTC) |
|---|---|---|---|---|---|---|
| Chronos-2 base (zero-shot) | `liander_chronos2-base` | 3 wx | 7-level | **0.0726** | **0.1016** | 2026-08-26 |
| Chronos-2 small (zero-shot) | `liander_chronos2-small` | 3 wx | 7-level | 0.0739 | 0.1036 | 2026-08-26 |
| XGBoost (weekly retrain) | `liander_xgboost` | all 11 wx + engineered | 7-level | 0.0946 | 0.1107 | 2026-08-25 |
| GBLinear (weekly retrain) | `liander_gblinear` | all 11 wx + engineered | 7-level | 0.0947 | 0.1312 | 2026-08-26 |
| TimesFM 2.5 (zero-shot) | `liander_timesfm` | none (univariate) | deciles | 0.1437 | 0.1928 | 2026-08-26 |
| Moirai 2.0 R small (zero-shot) | `liander_moirai` | 3 wx | deciles | 0.1421 | 0.1856 | 2026-08-27 |
| Chronos-2 base, all-wx variant | `liander_chronos2-base-allwx` | all 11 wx | 7-level | **0.0711** | **0.0987** | 2026-08-28 |
| TimesFM 2.5 + XReg | `liander_timesfm-cov` | 3 wx via XReg | deciles | 0.0894 | 0.1185 | 2026-08-27 |
| TimesFM 2.5 + XReg, all-wx | `liander_timesfm-allwx` | all 11 wx via XReg | deciles | 0.0848 | 0.1116 | 2026-08-27 |
| Moirai 2.0, all-wx variant | `liander_moirai-allwx` | all 11 wx | deciles | 0.1416 | 0.1868 | 2026-08-29 |
| XGBoost + conformal calibration | `liander_xgboost-cqr` | all 11 wx + engineered | 7-level | 0.0834 | 0.1126 | 2026-08-31 |
| TimesFM 3.0 (zero-shot)† | `liander_timesfm3` | none (univariate) | deciles | 0.1301 | 0.1793 | 2026-09-02 |
| TimesFM 3.0 (zero-shot)† | `liander_timesfm3-cov` | 3 wx, native | deciles | 0.0708 | 0.0954 | 2026-09-03 |
| TimesFM 3.0 (zero-shot)† | `liander_timesfm3-allwx` | all 11 wx, native | deciles | **0.0657** | **0.0892** | 2026-09-03 |

† TimesFM 3.0 (330M, `google/timesfm-3.0-pytorch`) weights are under the
TimesFM Non-Commercial License v1.0 — benchmark/evaluation use only, never
production. Chronos-2 remains the best production-licensed model in the
table.

### Per-park rCRPS (global window)

| model | Alphen | Dronten | Opmeer | Leeuwarden | Arnhem |
|---|---|---|---|---|---|
| chronos2-base | 0.0614 | 0.0680 | 0.0840 | 0.0791 | 0.0705 |
| chronos2-small | 0.0630 | 0.0682 | 0.0853 | 0.0804 | 0.0727 |
| xgboost | 0.0813 | 0.0877 | 0.1145 | 0.0926 | 0.0968 |
| gblinear | 0.0824 | 0.0860 | 0.1169 | 0.0981 | 0.0902 |
| timesfm | 0.1329 | 0.1351 | 0.1722 | 0.1368 | 0.1416 |
| moirai | 0.1329 | 0.1329 | 0.1665 | 0.1374 | 0.1408 |
| chronos2-base-allwx | 0.0605 | 0.0657 | 0.0838 | 0.0730 | 0.0725 |
| timesfm-cov | 0.0806 | 0.0838 | 0.1024 | 0.0924 | 0.0877 |
| timesfm-allwx | 0.0744 | 0.0784 | 0.0984 | 0.0899 | 0.0831 |
| moirai-allwx | 0.1323 | 0.1324 | 0.1662 | 0.1366 | 0.1404 |
| xgboost-cqr | 0.0702 | 0.0777 | 0.1021 | 0.0831 | 0.0838 |
| timesfm3 | 0.1232 | 0.1221 | 0.1527 | 0.1219 | 0.1306 |
| timesfm3-cov | 0.0606 | 0.0662 | 0.0819 | 0.0753 | 0.0700 |
| timesfm3-allwx | 0.0561 | 0.0607 | 0.0777 | 0.0668 | 0.0671 |

Run notes:

- **Chronos-2** uses the `DYNAMIC` checkpoint variant (the static-shape
  macOS/CoreML variant freezes the batch dimension and rejects batched
  backtest inference) and the official example's 3 covariates
  (`shortwave_radiation`, `wind_speed_80m`, `temperature_2m`).
- **XGBoost/GBLinear** are the OpenSTEF 4 presets: trained in-backtest with
  weekly retrain, full covariate set plus the presets' engineered features;
  6,090 vintages (30 fewer than zero-shot models — early-window training
  requirement).
- **TimesFM 2.5** (200M, torch) and **Moirai 2.0 R small** emit native
  deciles only, so they write to the `forecasts_deciles` instance (declared
  band {0.1, 0.3, 0.5, 0.7, 0.9}) and their rCRPS is computed over a
  narrower band than Chronos-2's — disclosed wherever compared. Both
  forecast *through* the ~48h publication gap (gap + horizon ≤ 512 decode
  steps) rather than forward-filling stale context.
- **Moirai** is installed from uni2ts PR #256's branch (main pins
  numpy~=1.26/torch<2.5, incompatible with openstef-core).

### The covariate-access findings (variants completed 2026-08-27 → 08-29)

The four variant runs isolate "input access" from "model class" — same
window, same targets, only the covariates change:

- **The covariate ladder dominates.** TimesFM at 0 → 3 → 11 covariates:
  0.1437 → 0.0894 → 0.0848 rCRPS — a 41% improvement from input access
  alone, through a *linear* side-channel (XReg in-context ridge; the
  transformer never sees the covariates). Covariate-fed TimesFM leapfrogs
  both trained classical presets.
- **Saturation is model-dependent.** Chronos-2 gained ~2% from the extra 8
  columns (0.0726 → 0.0711, the new overall leader); Moirai gained ~0.4%
  (0.1421 → 0.1416, within noise — 44h of compute for a null result, its
  any-variate sequence length scaling ~7× per extra-covariate load).
- **Every covariate-fed TSFM beats the trained presets**; the gap between
  the best TSFM (Chronos-2) and the rest is model class, not input access.

### The covariate-mechanism finding (TimesFM 3.0, 2026-09-02/03)

TimesFM 3.0 (released 2026-08-31; natively multivariate via
``past_future_covariates``) run at the same 0/3/11 covariate rungs as 2.5
gives a paired ladder that separates three effects for the first time:

- **Checkpoint generation** (univariate vs univariate): 0.1437 → 0.1301,
  ~9.5% — real but modest, and still far behind every covariate-fed row.
- **Covariate access** (each family's own ladder): still the dominant
  lever — 3.0 improves 49% from input access alone (0.1301 → 0.0657).
- **Covariate mechanism** (XReg side-channel vs native attention at equal
  input): the 2.5→3.0 gap *widens* from 9.5% univariate to ~21% at 3
  covariates and ~23% at 11 — native attention over covariates is worth
  roughly an extra 11–13 points beyond the generation upgrade.

`timesfm3-allwx` (0.0657 rCRPS / 0.0892 rMAE) is the **new overall
leader**, ahead of chronos2-base-allwx (0.0711 / 0.0987) on both metrics —
the rMAE comparison is band-independent, so the lead is not a
narrower-band artifact (the rCRPS basis caveat still applies). It won on
all five parks. Note the license asterisk above: the leader is
non-commercial; the best *production-licensed* entry is still Chronos-2.

### The band-calibration finding (`xgboost-cqr`, 2026-08-31)

The presets' rCRPS gap was diagnosed from the table itself: xgboost and
gblinear score nearly identical rCRPS (0.0946 / 0.0947) despite a 16% rMAE
gap — a much better median that buys no distributional skill means the
quantile *spread* is the bottleneck. `xgboost-cqr` tests it: the same
preset wrapped with openstef's `ConformalizedQuantileCalibrator`
(scripts/cqr_forecaster.py), fitted honestly — split-conformal on a
held-out 14-day window replayed with production knowledge cuts, never
in-sample (stock postprocessing fits on training predictions, which
gradient boosting's optimistic residuals would neuter).

Result: **rCRPS −11.8%** (0.0946 → 0.0834, improved on all 5 parks, −10%
to −14%) while **rMAE stayed put** (0.1107 → 0.1126; the slight rise is
the 14 training days sacrificed to the calibration split — the transform
never touches the median). Confirmed: the preset's raw band is drastically
overconfident (the corrections widen q05–q95 ~5× early on), and roughly
half the classical-vs-TSFM rCRPS gap was calibration, not forecasting
skill. Calibrated XGBoost is the best non-Chronos entry in the table,
ahead of covariate-fed TimesFM.

Caveats: TimesFM/Moirai rows are decile-band rCRPS (narrower basis);
`wind_direction_10m` enters raw despite being circular (degrees) — noted,
not addressed. Variant smoke tests live under scratch labels `smoke_*` /
`smoke2_*` (1 park × 2 days; ignore).

## External validation (researched 2026-09-01)

Are these results an outlier, or the classical baseline misconfigured?
Neither — checked against upstream and the published literature:

**Upstream replication.** OpenSTEF's own
[Benchmark Results page](https://openstef.github.io/openstef/user_guide/guides/benchmark_results.html)
reports, for the same wind_park targets: Chronos-2 rCRPS **0.063** vs
XGBoost **0.089** — a 29% gap, *larger* than ours (23%) — with the verbatim
takeaway "Chronos-2 … posts the lowest aggregate rCRPS and rMAE on this
benchmark." Our configs are copied from their official examples
(`examples/benchmarks/liander2024/`), their runner uses the same backtest
parameters (15-min sampling, 6h vintages, weekly retrain, D-1T06:00
availability), and the wind-park targets' official window is exactly ours
(2024-03-01 + 306 days). Model ranking matches; small absolute offsets
remain unattributed (package/ONNX versions are candidates).

**Independent replication of the headline margin.** The FETS benchmark
([arXiv:2604.22328](https://arxiv.org/abs/2604.22328); 54 energy datasets,
17,010 experiments) finds covariate-fed Chronos-2 beats XGBoost by ~23%
median NRMSE (0.472 vs 0.611) — 32–34% on wind/PV — against an XGBoost
tuned far harder than the OpenSTEF preset (SHAP feature selection, 250
Optuna trials per quantile), which directly refutes "the baseline just
needed tuning." fev-bench
([arXiv:2509.26468](https://arxiv.org/abs/2509.26468), 100 held-out tasks)
shows Chronos-2 ~30% ahead of tuned CatBoost/statistical ensembles on
scaled quantile loss.

**The calibration finding replicates.** GBM quantile undercoverage is
documented in operational systems (NREL solar: 48–71% empirical coverage
at 90% nominal —
[arXiv:2510.15780](https://arxiv.org/html/2510.15780)); a Belgian
offshore-wind study finds CQR beats parametric gradient boosting by 12%
CRPS ([arXiv:2602.13010](https://arxiv.org/html/2602.13010)) — we measured
11.8%.

**The covariate ladder replicates.** UniWind
([arXiv:2607.01670](https://arxiv.org/html/2607.01670v1)): history-only
zero-shot TSFMs are >2× worse than NWP-fed models on wind — our
univariate-TimesFM result exactly. NWP-covariate dominance is
energy-forecasting consensus (GEFCom2014, Giebel & Kariniotakis 2017).

**Boundaries and asterisks.** Tuned task-specific models still win
univariate evaluations, monthly/quarterly series, M5-style retail
(zero-shot Chronos loses ~4.7× WRMSSE to LightGBM —
[arXiv:2507.22053](https://arxiv.org/abs/2507.22053)), finance, day-ahead
*prices* ([arXiv:2506.08113](https://arxiv.org/abs/2506.08113)) and system
*imbalance*
([ML6/Elia](https://www.ml6.eu/en/blog/chronos-2-meets-the-grid-forecasting-system-imbalance-with-a-time-series-foundation-model)).
Honest qualifiers on our own table: Chronos-2's pretraining corpus includes
wind/solar data (a "zero-shot" asterisk); upstream's GBLinear+LightGBM
*ensemble* (not run here) narrows to within 5% of Chronos-2, so the fair
claim is "beats the individual trained presets," not "beats everything
classical"; and our GBLinear's rank diverges from upstream's (they show it
clearly ahead of XGBoost on wind rCRPS, we show a tie) — unresolved, don't
lean on that row.

## Planned / candidate benchmarks

- **Remaining liander2024 target groups** — the dataset carries ~50 further
  targets beyond the 5 wind parks; would test the convention (and the
  models) on load profiles other than wind.
- **Ensemble / composed forecasts** — combine the strong runs (e.g.
  chronos2-base + xgboost); also the store-design test for the open
  "composed forecasts" question in the spec (§11).
- **A genuinely revising source** — every actual in this benchmark is
  publish-once, so revision handling is validated by tests, not by nature;
  imbalance prices or settlement data would exercise the revisioned-actuals
  path for real.

## Superseded / scratch labels in the store

- `liander_chronos2` — early single-park pilot (28 vintages, Arnhem,
  2026-08-25) from before the base/small naming split. Superseded by
  `liander_chronos2-base`; kept as history.
- `smoke_*`, `smoke2_*` — 2-day wiring tests for the covariate variants
  (2026-08-27). Not results; excluded from every `liander\_%` report by the
  label filter.
