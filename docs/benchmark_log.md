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

### Per-park rCRPS (global window)

| model | Alphen | Dronten | Opmeer | Leeuwarden | Arnhem |
|---|---|---|---|---|---|
| chronos2-base | 0.0614 | 0.0680 | 0.0840 | 0.0791 | 0.0705 |
| chronos2-small | 0.0630 | 0.0682 | 0.0853 | 0.0804 | 0.0727 |
| xgboost | 0.0813 | 0.0877 | 0.1145 | 0.0926 | 0.0968 |
| gblinear | 0.0824 | 0.0860 | 0.1169 | 0.0981 | 0.0902 |
| timesfm | 0.1329 | 0.1351 | 0.1722 | 0.1368 | 0.1416 |
| moirai (3/5 parks) | — | — | 0.1665 | 0.1374 | 0.1408 |

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

## In progress

- **`liander_moirai`** — Moirai 2.0 R small, zero-shot, 3 wx covariates
  (`feat_dynamic_real`), decile band. 3/5 parks complete as of 2026-08-27:
  Arnhem 0.1408, Opmeer 0.1665, Leeuwarden 0.1374 (interim avg 0.1482; on
  the matched 3 parks TimesFM averages 0.1502, so covariate-fed Moirai is
  marginally ahead of univariate TimesFM). ~1h30m per park.

## Queued (launching automatically when the Moirai run exits)

Covariate-access variants, isolating "model class" from "input access".
Same window, same targets; fastest first:

| model | what changes | why |
|---|---|---|
| `timesfm-cov` | TimesFM + the 3 official covariates via its XReg path (in-context ridge regression; the transformer stays univariate) | TSFM comparison at equal input: timesfm vs moirai vs chronos2, all at 3 covariates |
| `timesfm-allwx` | TimesFM + all 11 weather columns via XReg | covariate-access axis for TimesFM |
| `chronos2-base-allwx` | Chronos-2 base fed all 11 (deviates from the official example's 3) | does the best model gain from the extra 8 columns? |
| `moirai-allwx` | Moirai fed all 11 (native any-variate attention) | same axis for Moirai; **slow**: ~4–8× per vintage vs 3-covariate (sequence length scales with variates), est. ~1 day |

Smoke-tested 2026-08-27 (1 park × 2 days each, labels `smoke_*` /
`smoke2_*` — scratch, ignore). Known caveat: `wind_direction_10m` enters
raw; it is circular (degrees), which models consume poorly — noted, not yet
addressed.

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
