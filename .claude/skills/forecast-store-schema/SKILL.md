---
name: forecast-store-schema
description: Design a forecast store schema on Postgres/TimescaleDB — the tables, keys, defaults, and queries for storing forecasts with leakage-free backtesting, reproducible evaluation, and vintage history. Use when asked to store forecasts/predictions, backtest without leakage, track forecast accuracy, keep forecast versions/vintages, or evaluate models or vendors in SQL. Plain SQL; no library required.
---

# Designing a forecast store schema

A forecast store is a **tri-temporal** store for forecasts and the data they are
made from and scored against. Two rules generate everything below:

1. **Every row carries three clocks.** `target_time` (what the value is about),
   `available_at` (when it became knowable — a writable domain claim), and
   `recorded_at` (when the database wrote it — `DEFAULT now()`, **never written
   by clients**). Rule: a column may sound like system time only if the
   database stamps it.
2. **It is a log of beliefs, not a table of values.** Tables are append-only; a
   revision or a new forecast vintage is a new row, never an UPDATE.

Full DDL to apply: [references/canonical-ddl.sql](references/canonical-ddl.sql)
(generated; includes the TimescaleDB hypertable/columnstore layer). The
normative contract is `docs/forecast-store-convention.md`; reasoning per rule is
`docs/forecast-store-rationale.md`. A Python generator/SDK exists
(`pip install forecast-store`) but nothing below requires it.

## The design procedure

### 1. Declare each series (quantity) in the registry

One row per measured or forecasted quantity. The declared `sample_interval` is
the series' **grid**: every `target_time` for that series must be a bucket
boundary at that resolution (a timestamp here names an interval, not a point —
that exact-equality is what makes revisions resolvable and evaluation an
equi-join). Resolve names strictly: `forecast.get_series_id()` raises on
unknown names so typos fail at write time; `forecast.register_series()` is the
get-or-create path.

### 2. Classify every stream by the realization test

Every quantity has a **realization moment** — the instant its true value
becomes fixed (the meter interval ends; the auction clears). Classify each
stream by which side of it rows are written on:

- Written **after** realization → a *measurement*: mis-measured but never
  wrong; a newer row **corrects** it → `actuals`.
- Written **before** realization → a *prediction*: wrong but never corrected; a
  newer row **re-predicts** → `predictors` (producer opaque, e.g. a vendor
  feed) or `forecasts` (producer known: a run with model, params, context).

Timestamps do not decide this. The day-ahead auction price publishes before
delivery yet realizes when the auction clears — a *measurement* with
`available_at` before `target_time`. A desk's forecast *of* that price is a
prediction. One quantity may hold both kinds of belief, which is why the kind
lives in the table address, never on the series or as a row label.

### 3. For actuals: does the source revise?

Same columns either way; **the primary key is the switch**:

- **Revisioned** — `PRIMARY KEY (series_id, target_time, available_at)` — for
  settlement-grade sources that restate. Idempotent ingest is
  `ON CONFLICT DO NOTHING`: the key names a belief's full coordinates, so a
  colliding different value is a retcon, refused first-wins; a real correction
  arrives under a new `available_at`.
- **Single-belief** — `PRIMARY KEY (series_id, target_time)` — only for
  pipelines you own where a second belief is a defect. Its write is
  `INSERT .. ON CONFLICT DO UPDATE SET value = EXCLUDED.value` under the
  `belief_guard` trigger (in the DDL): identical re-delivery is a silent no-op,
  a conflicting value **raises** — never silently swallowed.

Trap: "never revises" is a claim about the world, and the world restates
(exchanges republish "final" prices). A publish-once *external* feed belongs in
the revisioned shape, where a restatement lands as data and alarms.

### 4. Get the `available_at` contract right per table

Same column, opposite defaults — both load-bearing:

- `actuals.available_at DEFAULT now()`: arrival is measured; the lazy write is
  the honest write (forcing writers to fill it invites `target_time` or client
  clocks — fabricated zero-lag knowledge). Backfills with genuine historical
  availability **state it** per row.
- `predictors.available_at NOT NULL, no default`: publication time must be
  stated — defaulting it to arrival corrupts lead-time attribution (a 06:00
  forecast stamped at its 08:00 arrival scores two hours fresher than it was).
  `recorded_at − available_at` then measures vendor delivery lag.

### 5. Declare the quantile band per forecast-log instance

Probabilistic forecasts are wide typed columns named bijectively from levels:
`q05` ↔ 0.05, `q50` ↔ 0.5, `q02_5` ↔ 0.025. Never store a mean in `q50`
(a mean is not a median); vendors' point column is `value` because their
statistic may be unknown. A model emitting a different band gets its own
forecast-log instance whose declared band is true — never NULL-padded columns
in another instance's band. Every instance self-declares in
`forecast.store_tables` (columns, knowledge clock, band, role), so any client
can reconstruct the store's shape from the store alone.

### 6. Apply the DDL

Run [references/canonical-ddl.sql](references/canonical-ddl.sql). On
TimescaleDB it includes hypertables, columnstore (segmentby `series_id`,
orderby the two clocks — the minmax sparse indexes are exactly the as-of
predicates), and a generated `data_quality_sweep(scan_window)` function
(orphan ids, off-grid `target_time`; schedule it with cron or `add_job`).
Integrity is monitor-first: no FKs on points tables (per-row write tax);
the strict resolver + the sweep cover it.

## Canonical queries

**As-of (the workhorse — serving, covariates, backtest inputs).** Never read a
belief log without a knowledge cutoff:

```sql
SELECT DISTINCT ON (target_time) target_time, available_at, q50
FROM forecast.forecasts
WHERE series_id = forecast.get_series_id('site42/load')
  AND target_time BETWEEN :t0 AND :t1
  AND available_at <= :asof            -- the knowledge cutoff
ORDER BY target_time, available_at DESC;
```

**Evaluation join (score what would have been used).** Pick the vintage first,
then join to truth; the `recorded_at` pin makes it reproducible:

```sql
WITH ef AS (
    SELECT DISTINCT ON (f.series_id, f.target_time)
           f.series_id, f.target_time, f.q50
    FROM forecast.forecasts f
    WHERE f.available_at <= f.target_time - interval '12 hours'  -- gate closure
      AND f.recorded_at  <= :frozen_at                           -- the pin
    ORDER BY f.series_id, f.target_time, f.available_at DESC
), ea AS (
    SELECT DISTINCT ON (series_id, target_time) series_id, target_time, value
    FROM forecast.actuals
    WHERE available_at <= now() AND recorded_at <= :frozen_at
    ORDER BY series_id, target_time, available_at DESC
)
SELECT ef.series_id, avg(abs(ef.q50 - ea.value)) AS mae
FROM ef JOIN ea USING (series_id, target_time)
GROUP BY 1;
```

Pin placement is the semantics: pin = `now()` → live monitoring; pinned once
at experiment time → reproducible forever; `recorded_at <= S` per simulated
origin → the model *as operated* (ingestion delays included); pinned at one
decision → forensics ("what did we know at bid time").

**Current forecast**: the generated `forecast.latest_<table>` view.

## Invariants (check every design against these)

- `recorded_at` is never written by a client, on any table.
- Every belief-log read states its `asof`; frozen reads pin `recorded_at` too.
- Append-only; the one sanctioned DELETE is label-scoped benchmark-workspace
  replacement.
- Never `ON CONFLICT DO NOTHING` on a single-belief table (it silently
  swallows conflicting values, unrecoverably).
- `target_time` is always on the series' declared grid; the sweep audits.
- `mean` is never spelled `q50`; a band column exists only if the producer
  emits that level.
- One quantity = one series, across all tables; wideness is reserved for the
  distribution, never for multiple quantities.
