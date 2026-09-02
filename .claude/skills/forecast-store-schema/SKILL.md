---
name: forecast-store-schema
description: Design a forecast store schema on Postgres/TimescaleDB — the tables, keys, defaults, and queries for storing forecasts with leakage-free backtesting, reproducible evaluation, and vintage history. Use when asked to store forecasts/predictions, backtest without leakage, track forecast accuracy, keep forecast versions/vintages, or evaluate models or vendors in SQL. Plain SQL; no library required.
---

# Designing a forecast store schema

A **forecast store** holds forecasts, the input data they were made from, and the
observations they are scored against, in plain Postgres/TimescaleDB tables. It makes
three things teams otherwise hand-roll easy and correct:

- **Vintages.** A forecast made Monday for Wednesday and a forecast made Tuesday for the
  same Wednesday are different records. Both are kept; either can be retrieved.
- **Leakage-free backtesting.** A backtest must see only data that was knowable at the
  simulated forecast time. *Leakage* — accidentally reading later data — inflates
  backtest accuracy and evaporates in production.
- **Accuracy as data.** Scoring forecasts against realized values is a SQL join, so
  accuracy is a queryable time series, not a notebook artifact.

The normative contract is `docs/forecast-store-convention.md`; per-rule reasoning is
`docs/forecast-store-rationale.md`. A Python generator/SDK exists
(`pip install forecast-store`) but nothing below requires it.

## Two rules generate everything

**Rule 1 — every row carries three clocks.** Forecast data is **tri-temporal**:
bi-temporal modeling (valid time + transaction time, per SQL:2011) is not enough,
because *when the source knew it* is a third, independent time. Three timestamp
columns, on every table, answering three different questions:

| Column | Question it answers | Who sets it |
|---|---|---|
| `target_time` | What moment is the value *about*? | the writer |
| `available_at` | When did the value become *knowable*? | the writer (a domain claim) |
| `recorded_at` | When did *this database* write the row? | `DEFAULT now()` — never the client |

The three genuinely differ. A Monday weather forecast *about* Wednesday is knowable
Monday (`available_at` before `target_time`). A meter reading *about* Monday arriving
through a 48-hour feed is knowable Wednesday (`available_at` after `target_time`). If
that reading is backfilled next month, `recorded_at` is next month. Filtering on
`available_at <= t` simulates what was knowable at time `t`; `recorded_at` is the
unforgeable witness that keeps the simulation honest, because `available_at` is writable
by design. Naming rule: a column may *sound* like system time only if the database
stamps it.

**Rule 2 — tables are append-only: a log of beliefs, not a table of values.** Every row
is a **belief**: "value v about moment T, held as of moment A". A correction or a newer
forecast is a new row, never an UPDATE. A **vintage** is the set of beliefs that were
current at some knowledge time; the **as-of query** (below) selects one by cutting on
`available_at`. Append-only is what makes vintages, as-of reads, and reproducible
evaluation fall out of the schema with no versioning machinery.

## The tables

Beliefs live in three kinds of points table, split by what a row *is*, not what it
measures. The names below are just the defaults — what identifies a table is the
**role** declared in its `store_tables` row, and a store may hold several instances of
a role under any names (e.g. a second forecast table with a different quantile band):

- **`forecast.actuals`** (role `actuals`) — measurements: rows written *after* the
  value became fixed.
- **`forecast.predictors`** (role `predictors`) — predictions from an opaque producer
  (e.g. a vendor's weather feed): all anyone knows is (target, publication time, value).
- **`forecast.forecasts`** (role `forecasts`) — run-bearing predictions, yours or any with provenance, each tied to a
  `forecast.runs` row recording the model, parameters, and input window that produced it.

Two metadata tables make the store self-describing: **`forecast.series`** (the registry:
one row per measured or forecasted quantity, carrying its declared resolution) and
**`forecast.store_tables`** (one row per points table declaring its role, columns, and
quantile band — any client can reconstruct the store's shape from the store itself).

## The design procedure

Steps 1–4 are design decisions — no SQL runs. Step 5 creates every table. Registering
series and writing data (steps 6–7) is only possible after step 5, because the registry
and points tables have to exist first.

### 1. Inventory the quantities and pick each one's grid

Plan one registry entry per *quantity* (site 42's load, site 42's irradiance, the FR
day-ahead price). For each, decide:

- **Name** — an immutable machine identifier, conventionally a slash path
  (`site42/load`, `site42/wx/wind_speed_80m`); human naming goes in `description`.
- **Grid** — the `sample_interval`. Every `target_time` for the series must be a bucket
  boundary at this resolution: a timestamp here *names an interval*, not a point ("the
  load at 18:00" means "average power over [18:00, 18:15)"). Exact alignment makes "the
  same target" an equality — a correction supersedes its predecessor instead of becoming
  a near-duplicate sibling, and evaluation is a plain equi-join. If a source emits
  off-grid timestamps, ingest snaps them to the grid.

Know the interval from the source's contract; never infer it from observed data spacing.
One quantity = one series, even when several tables hold beliefs about it — measured
load in `actuals`, a vendor's load prediction in `predictors`, your model's in
`forecasts`, all under one `series_id`.

### 2. Classify every stream by the realization test

Every quantity has a **realization moment** — the instant its true value becomes fixed
(the meter interval ends; the auction clears). Classify each stream by which side of
that moment its rows are written on:

- Written **after** realization → a *measurement*: it can be mis-measured but never
  wrong; a newer row **corrects** it → `actuals`.
- Written **before** realization → a *prediction*: it can be wrong but is never
  corrected; a newer row **re-predicts** from newer information, and every superseded
  vintage stays individually scoreable → `predictors` (producer opaque) or `forecasts`
  (producer known: a run with model, params, context).

Timestamps do not decide this. The day-ahead auction price publishes before delivery yet
realizes when the auction clears — a *measurement* whose `available_at` precedes its
`target_time`. Your desk's forecast *of* that price is a prediction. One quantity may
hold both kinds of belief, which is why the kind lives in the table address, never on
the series or as a row label.

This classification is load-bearing, not taxonomy — everything downstream derives from
it: what a newer row for the same target *means* sets the revision rules (step 3), the
measurement/prediction split sets each table's `available_at` contract (step 7), and
evaluation always scores predictions against measurements (the join below).

### 3. For actuals: does the source revise?

Some sources restate history (settlement-grade meter data is corrected for weeks); for
others a second value for the same target is a bug. Same columns either way; **the
primary key is the switch**:

- **Revisioned** — `PRIMARY KEY (series_id, target_time, available_at)` — for sources
  that restate. A correction is a new row under a new `available_at`. Idempotent ingest
  is `ON CONFLICT DO NOTHING`: the key names a belief's full coordinates, so a colliding
  row with a *different* value would silently rewrite history — it is refused, first
  write wins.
- **Single-belief** — `PRIMARY KEY (series_id, target_time)` — only for pipelines you
  own, where a second belief is a defect to surface. Its write is
  `INSERT .. ON CONFLICT DO UPDATE SET value = EXCLUDED.value` under the `belief_guard`
  trigger (ships in the DDL): identical re-delivery is a silent no-op, a conflicting
  value **raises** with both values in the message — never silently swallowed.

Trap: "never revises" is a claim about the world, and the world restates (exchanges
republish "final" prices). A publish-once *external* feed belongs in the revisioned
shape, where a restatement lands as data and can alarm; reserve single-belief for
pipelines you own.

### 4. Choose the quantile band per forecast table

A probabilistic forecast is stored as wide typed columns, one per quantile level, named
by a fixed bijection: `q05` ↔ 0.05, `q50` ↔ 0.5, `q02_5` ↔ 0.025. Decide the band (the
set of levels) per forecast table; it is declared in that table's `store_tables` row and
the columns derive from it. Two rules:

- **Never store a mean in `q50`** — a mean is not a median. Your own forecast table gets
  an honest `mean` column; a vendor's point column is `value`, because their statistic
  (mean? median? deterministic run?) may be unknown — if it is known, it's registry
  metadata, not a column name.
- A model emitting a **different band** gets its own forecast table whose declared band
  is true — never NULL-padded columns in another table's band.

### 5. Create the tables: catalog once, then one block per stream

Now execute DDL, in this order:

1. **[references/catalog-ddl.sql](references/catalog-ddl.sql)** — paste once per store,
   first. The decision-invariant layer: the `series` registry and its resolver
   functions, the `store_tables` catalog, run provenance (`runs`), evaluation tables,
   guard functions, and `data_quality_sweep(scan_window)` (finds unregistered ids,
   off-grid `target_time`, out-of-bucket observations; schedule it with cron or
   TimescaleDB `add_job`). The sweep is catalog-driven — it discovers points tables from
   their `store_tables` rows at execution time — so nothing you decided in steps 2–4
   changes this file.
2. **[references/points-tables.sql](references/points-tables.sql)** — one self-contained
   block per stream you classified: the table, its index/view/triggers, its own
   `store_tables` declaration row, and its TimescaleDB layer (hypertable + columnstore:
   segmentby `series_id`, orderby the two clocks — the resulting minmax sparse indexes
   match exactly the as-of predicates). The default three blocks (forecasts, predictors,
   revisioned actuals) are the common case; the variant blocks are the step-3
   single-belief actuals and a step-4 second forecast table with its own band — derive
   further instances the same way, columns from the band by the bijection.

Because every block carries its own declaration row, adding a table to a live store =
executing its block: the sweep and any catalog-reading client pick it up immediately.
Integrity is monitor-first: no foreign keys on points tables (per-row FK checks tax
bulk ingest); the strict resolver at write time plus the scheduled sweep cover it.

### 6. Register the series

The registry now exists. Register every series from step 1 — always through the
resolver functions, never a raw INSERT into `forecast.series`:

```sql
SELECT forecast.register_series('site42/load', interval '15 minutes');  -- get-or-create
SELECT forecast.get_series_id('site42/load');  -- strict: raises on unknown names
```

Use `get_series_id()` inside every write (`VALUES (forecast.get_series_id('site42/load'), ...)`)
so a typo'd series name fails loudly at insert time instead of landing as an orphan id.

### 7. Write data: each table's `available_at` contract

Never write `recorded_at`, on any table — it is always the column default. For
`available_at` the defaults differ per table, and both directions are load-bearing:

- **`actuals.available_at DEFAULT now()`** — a fact's knowledge time is when you learned
  it, and arrival is measurable. The default can only overstate lateness, never
  fabricate zero-lag knowledge, so the lazy write is the safe write. Backfills with
  genuine historical availability state it per row.
- **`predictors.available_at NOT NULL, no default`** — a prediction's knowledge time is
  its publication time, which only the writer can state. Defaulting it to arrival would
  score a vendor's 06:00 forecast delivered at 08:00 as two hours fresher than it was.
  `recorded_at − available_at` then measures vendor delivery lag.
- **`forecasts`** — write the run row and its points in **one transaction**. Production
  stamps `available_at = now()`; a backtest stamps the simulated decision moment (that
  writability is the point — and `recorded_at` makes the retro-stamping visible).

Idempotent ingest follows step 3's shape: revisioned → `ON CONFLICT DO NOTHING`;
single-belief → the `belief_guard` upsert. Never use `ON CONFLICT DO NOTHING` on a
single-belief table — it silently swallows conflicting values, unrecoverably.

## Canonical queries

**As-of (vintage selection)** — "what did we believe about these targets, as of moment
X?": per target, the row with the latest `available_at` at or before the cutoff. This is
the workhorse for serving, covariate assembly, and backtest inputs. Never read a belief
log without a knowledge cutoff — an uncut read means "everything I know now", which
inside a backtest is leakage:

```sql
SELECT DISTINCT ON (target_time) target_time, available_at, q50
FROM forecast.forecasts
WHERE series_id = forecast.get_series_id('site42/load')
  AND target_time BETWEEN :t0 AND :t1
  AND available_at <= :asof            -- the knowledge cutoff
ORDER BY target_time, available_at DESC;
```

The same shape against `predictors` is leakage-free feature assembly; with
`asof = now()` it is "current best forecast" (or use the generated
`forecast.latest_<table>` view for the hot path).

**Evaluation join** — score the forecast that *would have been used*. The cutoff is
relative to the operational decision deadline (here "gate closure": day-ahead markets
close ~12h before delivery, so the forecast in force is the last one available by then).
The join's shape is step 2's split: predictions on the left, measurements (`actuals`)
on the right as truth — swap a `predictors` table in for `forecasts` and the same query
scores a vendor. Pick the vintage *first*, then join to truth:

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

The two cutoffs do different jobs. `available_at` cutoffs simulate what was *knowable*
at each decision moment. The **pin** — `recorded_at <= :frozen_at` — freezes the dataset
as the store physically held it at one instant, so re-running with the same pin returns
identical rows forever, even after backfills and revisions land. Because `available_at`
is a writable claim, the pin must guard **every** belief-log read in the query,
forecasts included. Pin placement is the semantics: pin = `now()` → live monitoring;
pinned once at experiment start → reproducible forever; `recorded_at <= S` per simulated
origin `S` → the model *as operated*, ingestion delays included; pinned at one
decision's timestamp → forensics ("what did we know at bid time").

## Invariants (check every design against these)

- `recorded_at` is never written by a client, on any table.
- Every belief-log read states its knowledge cutoff; frozen reads pin `recorded_at` too.
- Append-only. The one sanctioned DELETE: re-running a benchmark may delete-then-insert
  rows scoped to its own run label — production history is never deleted or updated.
- Never `ON CONFLICT DO NOTHING` on a single-belief table.
- `target_time` is always on the series' declared grid; the sweep audits.
- `mean` is never spelled `q50`; a band column exists only if the producer emits that
  level.
- One quantity = one series, across all tables; wide columns are reserved for the
  quantile distribution, never for multiple quantities.
