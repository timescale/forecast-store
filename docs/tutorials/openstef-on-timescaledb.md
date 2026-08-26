# OpenSTEF on TimescaleDB: a forecast store in six steps

Run [OpenSTEF](https://github.com/OpenSTEF/openstef) — LF Energy's production-proven
short-term energy forecasting pipeline — with TimescaleDB as its storage layer. By the
end of this tutorial you will have:

1. provisioned a **forecast store** (a bi-temporal schema convention on Postgres/TimescaleDB),
2. ingested real Dutch grid data — 15-minute wind-park load measurements with their real
   ~48-hour publication lags, plus the full revision history of the weather forecasts,
3. reproduced OpenSTEF's own Chronos-2 benchmark with the database as the *only* data
   source and the *only* result sink,
4. queried forecast vintages and accuracy as ordinary SQL rows, and
5. wired a production workflow so every prediction it makes is persisted with honest
   knowledge time.

**Why this exists.** OpenSTEF 4.x is explicit that storage is your responsibility: the
3.x database layer has no 4.x equivalent, and the migration guide's contract is "you own
I/O." OpenSTEF is also rigorously bi-temporal *in memory* — every dataset row carries an
`available_at` and the backtest engine enforces knowledge cutoffs — but nothing ships
that stamps or persists that knowledge time. In practice teams fill the hole with parquet
directories and hand-rolled tables. This tutorial fills it with one Postgres database
instead: indexed queries over every forecast ever made, leakage-free backtests, and
accuracy as a queryable time series. The schema convention itself is documented in
[the forecast store convention](../forecast-store-convention.md); the adapter reference
is [docs/integrations/openstef.md](../integrations/openstef.md).

## Prerequisites

- **A Postgres database**, ideally with TimescaleDB (a free
  [Tiger Cloud](https://console.cloud.timescale.com/) service works; so does
  self-hosted TimescaleDB, or plain Postgres — the store degrades gracefully to
  ordinary tables).
- **Python ≥ 3.12** and [uv](https://docs.astral.sh/uv/).
- ~2 GB of disk for the Chronos-2 ONNX checkpoint and the benchmark parquets
  (downloaded from HuggingFace on first run).

```bash
git clone <forecast-store repo> && cd forecast-store
echo 'FORECAST_STORE_TEST_DSN=postgres://user:password@host:port/dbname?sslmode=require' > .env
```

The scripts and tests read the DSN from `.env`.

## Step 1 — Provision the store

```bash
uv run forecast-store provision --dsn "$FORECAST_STORE_TEST_DSN"
```

One command creates the `forecast` schema: a **series registry** (`series` — one row per
measured or forecasted quantity, with an authoritative `sample_interval`), a
**self-description table** (`store_tables` — one row per points table, declaring its
value columns and knowledge clock, so any client can reconstruct the store's shape from
the store alone), and the points tables:

| Table | Holds | Knowledge model |
|---|---|---|
| `actuals` | measurements, possibly revised | rows written *after* the target realizes |
| `predictors` | external forecast vintages (weather, prices) | rows written *before* — one per publication |
| `forecasts` | your own models' output, with run provenance | `runs` records who/what/when/from-which-inputs |
| `evaluation_*` | accuracy results | metrics as queryable rows |

Every points table carries the same three clocks: `target_time` (the moment the row is
*about*), `available_at` (when the value became knowable — the writable domain claim),
and `recorded_at` (when the store wrote it — database-stamped, never client-written).
On TimescaleDB each table is also a hypertable with columnstore compression configured.
Re-running `provision` is safe: it verifies the stored declarations instead of mutating
anything, and it can upgrade a plain-Postgres store in place after you install the
extension.

To inspect instead of execute: `uv run forecast-store ddl --timescale` prints the full
DDL.

## Step 2 — Ingest real grid data

```bash
uv run --extra openstef --extra foundation python scripts/ingest_liander.py
```

This pulls one target from the public
[liander2024 benchmark dataset](https://huggingface.co/datasets/OpenSTEF/liander2024-stef-benchmark)
(a normalized wind park near Arnhem–Nijmegen, published by the Dutch DSO Alliander) and
writes ~35,000 load measurements into `actuals` and ~591,000 weather-forecast vintage
rows into `predictors`:

```
actuals  ln24/wind_park/within_stadsregio_arnhem_nijmegen_normalized/load: 35133 rows (real per-row claims)
predictors  ln24/wind_park/within_stadsregio_arnhem_nijmegen_normalized/wx/temperature_2m: 196963 vintage rows
...
```

Two things make this dataset an honest stress test rather than a toy:

**The measurements carry real publication lags.** Every load row has a per-row
`available_at` from the dataset itself — the liander feed publishes roughly 48 hours
after the fact. Check:

```sql
SELECT avg(available_at - target_time) AS publication_lag
FROM forecast.actuals;
```
```
 publication_lag
-----------------
 2 days
```

**The weather arrives as vintages.** Each target time was forecast repeatedly as the
weather models re-ran:

```sql
SELECT target_time, count(*) AS vintages
FROM forecast.predictors
WHERE series_id = forecast.get_series_id(
        'ln24/wind_park/within_stadsregio_arnhem_nijmegen_normalized/wx/wind_speed_80m')
GROUP BY target_time ORDER BY vintages DESC, target_time LIMIT 3;
```
```
      target_time       | vintages
------------------------+----------
 2024-02-20 06:00:00+00 |        6
 2024-02-20 06:15:00+00 |        6
 2024-02-20 06:30:00+00 |        6
```

Up to 6 beliefs per target time — all kept. A newer weather run doesn't *correct* the
older one; it re-predicts from newer information, and the store keeps every vintage
because each is individually scoreable.

Note what the ingest did *not* do: no timestamp munging, no "latest value wins"
flattening. `COPY ... (series_id, target_time, available_at, value)` — the bi-temporal
structure of the source data maps 1:1 onto the store.

## Step 3 — Time travel: the as-of read

Everything the store does downstream rests on one query shape: *the latest belief about
each target time, as of a knowledge cutoff*. This is OpenSTEF's `select_version()` /
`filter_by_available_before()`, as SQL:

```sql
-- What did we believe the wind speed would be, as of the day-ahead
-- gate closure (06:00 the day before delivery)?
SELECT DISTINCT ON (target_time) target_time, available_at, value
FROM forecast.predictors
WHERE series_id = forecast.get_series_id(
        'ln24/wind_park/within_stadsregio_arnhem_nijmegen_normalized/wx/wind_speed_80m')
  AND target_time >= '2024-06-04' AND target_time < '2024-06-05'
  AND available_at <= '2024-06-03 06:00+00'   -- the knowledge cutoff
ORDER BY target_time, available_at DESC;
```

Change the cutoff and the answer changes with it — that is the whole point. The same
cutoff applied to `actuals` shows the 48-hour lag doing its work:

```sql
SELECT max(target_time) FROM forecast.actuals
WHERE available_at <= '2024-06-08 06:00+00';
```
```
 2024-06-06 06:00:00+00
```

As of June 8 at 06:00, the most recent *measurement you could actually have had* was 48
hours old. A backtest that reads through this cutoff cannot train on data that hadn't
been published yet — not because the harness is careful, but because the query cannot
return it.

In Python, `StoreReader` assembles a model-ready OpenSTEF `TimeSeriesDataset` the same
way — measured history up to the decision moment, covariates as-of it, gapfilled on the
registry-declared grid:

```python
from datetime import datetime, timedelta, timezone
from forecast_store.integrations.openstef import StoreReader

BASE = "ln24/wind_park/within_stadsregio_arnhem_nijmegen_normalized"
asof = datetime(2024, 6, 8, 6, 0, tzinfo=timezone.utc)   # the decision moment

dataset = StoreReader(DSN).context(
    target_series=f"{BASE}/load",
    covariates={
        "temperature_2m":      f"{BASE}/wx/temperature_2m",
        "wind_speed_80m":      f"{BASE}/wx/wind_speed_80m",
        "shortwave_radiation": f"{BASE}/wx/shortwave_radiation",
    },
    history_start=asof - timedelta(days=30),
    asof=asof,
    horizon_end=asof + timedelta(days=3),   # covariates extend into the horizon
)
dataset.store_context   # provenance: covariates_asof, per-column sources, gap stats
```

The covariate columns extend three days past `asof` (future *target* times from
already-published forecasts — knowledge still bounded by `asof`); the load column ends
where published measurements end. The `store_context` attribute records exactly what was
read and as-of when — it will follow the data into the forecast run's provenance in
Step 6.

## Step 4 — Run OpenSTEF's Chronos-2 benchmark against the store

```bash
uv run --extra openstef --extra foundation python scripts/run_liander_chronos2.py
```

This is OpenSTEF's own benchmark wiring — the Chronos-2 foundation model (zero-shot,
BASE checkpoint), a 7-quantile band, 3-day horizon, new forecast every 6 simulated
hours over a 7-day benchmark window — with exactly two substitutions:

- **`TimescaleTargetProvider`** replaces the parquet-directory provider: training and
  prediction data are served from `actuals` and `predictors` as versioned datasets, and
  the engine applies its own knowledge cutoffs against the stored `available_at`.
- **`TimescaleBenchmarkStorage`** replaces the filesystem sink: every simulated vintage
  becomes a `runs` row (with its simulated `available_at`) plus wide quantile rows in
  `forecasts`; the evaluation report lands in the `evaluation_*` tables.

No files are written. The run ends with a summary queried straight back out of the
store:

```
=== liander_chronos2/Within Stadsregio Arnhem Nijmegen_normalized ===
forecast runs (simulated vintages): 28
forecast points: 8092
             rCRPS @  global: 0.0906
              rMAE @     0.5: 0.1248
```

Because backtest vintages carry a *simulated* `available_at` in the past while
`recorded_at` is stamped at write time, retro-written rows are visible by construction —
and the run's provenance records that these are simulations. No separate database
needed.

## Step 5 — Query what a filesystem can't answer

**How did the forecast for one delivery hour evolve as it approached?** One query — this
is the shape that parquet directories make painful and the store makes trivial:

```sql
SELECT r.available_at, f.q10, f.q50, f.q90
FROM forecast.forecasts f JOIN forecast.runs r USING (run_id)
WHERE r.run_name = 'liander_chronos2/Within Stadsregio Arnhem Nijmegen_normalized'
  AND f.target_time = '2024-06-03 00:00+00'
ORDER BY r.available_at;
```
```
      available_at      |  q10   |  q50   |  q90
------------------------+--------+--------+-------
 2024-06-01 00:00:00+00 | -0.180 | -0.044 | 0.006
 2024-06-01 06:00:00+00 | -0.165 | -0.033 | 0.006
 2024-06-01 12:00:00+00 | -0.126 | -0.022 | 0.012
 ...
 2024-06-02 18:00:00+00 | -0.064 | -0.002 | 0.014
 2024-06-03 00:00:00+00 | -0.063 | -0.009 | 0.010
```

Nine vintages, the q10–q90 band tightening as the target approaches: 0.186 wide at the
two-day lead (first row: −0.180 to 0.006) down to 0.073 at delivery (last row: −0.063 to
0.010). Every number the model ever believed, addressable by SQL.

**Was it calibrated?** Accuracy lives in relational tables, not a report file:

```sql
SELECT s.metric, s.quantile, round(m.value::numeric, 4) AS value
FROM forecast.evaluation_metrics m
JOIN forecast.evaluation_series s USING (eval_series_id)
WHERE s.run_name LIKE 'liander_chronos2/%' AND s.win = 'global'
ORDER BY s.metric, s.quantile;
```
```
        metric        | quantile | value
----------------------+----------+--------
 observed_probability |     0.05 | 0.0761
 observed_probability |      0.1 | 0.1510
 observed_probability |      0.3 | 0.3746
 observed_probability |      0.5 | 0.5838
 observed_probability |      0.7 | 0.7812
 observed_probability |      0.9 | 0.9667
 observed_probability |     0.95 | 0.9952
 rCRPS                |   global | 0.0906
 rMAE                 |      0.5 | 0.1248
```

`observed_probability` is the calibration check: `q30 = x` claims the actual will land
at or below x 30% of the time (the *nominal* probability), and the observed value is how
often it actually did. Here every level runs near nominal (5% → 7.6%, …, 95% → 99.5%) —
the model's uncertainty bands mean what they claim, zero-shot, with a mild consistent
tilt (every observed slightly above nominal: the predictive distribution sits a touch
high). Because these are rows, the follow-ups are `WHERE` clauses: compare model labels
across runs, chart a metric over successive benchmark dates, point Grafana at it.

And that comparison scales. We ran the full official benchmark window — all 5 wind
parks × 306 days — for four models on identical store-served data
(`scripts/run_liander_benchmark.py`: 24,448 vintages, 10.5M forecast points), and the
whole result is one query away:

| model | avg rCRPS | avg rMAE@q50 |
|---|---|---|
| chronos2-base (zero-shot) | **0.0726** | **0.1016** |
| chronos2-small (zero-shot) | 0.0739 | 0.1036 |
| xgboost (weekly retrain) | 0.0946 | 0.1107 |
| gblinear (weekly retrain) | 0.0947 | 0.1312 |

Zero-shot foundation models beat the trained classical presets by ~23% rCRPS on
OpenSTEF's own benchmark — and note the detail only a table like this surfaces: xgboost
and gblinear tie on rCRPS while diverging on the median.

## Step 6 — The production write path

In production, OpenSTEF's `predict()` returns quantiles on a timestamp index — and
nothing records *when the forecast was made*. That is the single most important fact for
ever evaluating or reproducing it later, and `ForecastStoreCallback` is its recorder.
Attach it to any workflow:

```python
from openstef_models.workflows.custom_forecasting_workflow import CustomForecastingWorkflow
from forecast_store.integrations.openstef import ForecastStoreCallback

callback = ForecastStoreCallback(DSN, f"{BASE}/load")
workflow = CustomForecastingWorkflow(
    model=model,                        # any openstef-models ForecastingModel
    model_id="windpark-arnhem",
    run_name="prod/windpark-arnhem",
    callbacks=[callback],
)
workflow.fit(dataset)
result = workflow.predict(dataset)      # ← persisted: run + points, one transaction
```

On every `predict()` the callback stamps the real `available_at`, validates the model's
quantiles against the store's declared band, and writes one `runs` row (model identity,
input window, parameters — including the `store_context` provenance if the input came
from `StoreReader`, closing the leakage audit on both halves of the input) plus the
forecast points, atomically. Serving reads come off a generated view:

```sql
SELECT * FROM forecast.latest_forecasts WHERE series_name = 'ln24/.../load';
```

That's the full loop: context in from the store as-of the decision moment, predictions
out with honest knowledge time, evaluation joining the two — one database.

## Where to go next

- **Scale it**: `ingest_liander.py --all-targets` ingests a whole group, and
  `run_liander_benchmark.py` runs the four-model comparison above (`--models`,
  `--targets`, `--start`, `--days`); 50 more targets await in the solar, transformer,
  and feeder groups.
- **Keep experiments out of production history**: declare a second forecast-log instance
  (a backtest workspace with its own retention) via `StoreConfig(extra_tables=...)` —
  see the convention's §7.2.
- **The convention itself** — three clocks, the actuals/predictors split, quantile
  representation, the evaluation join: [forecast-store-convention.md](../forecast-store-convention.md).
- **Adapter internals and packaging notes**:
  [integrations/openstef.md](../integrations/openstef.md).
