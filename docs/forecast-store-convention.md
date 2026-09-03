# The Forecast Store Convention

**Design document — draft v0.4** · 2026-08-26 · `forecast-store`
Status: **spike-validated.** The design below has been implemented (generator, skill, SDK
read/write paths, OpenSTEF adapters) and validated live against OpenSTEF 4.x — including
the liander2024 Chronos-2 benchmark running end-to-end with a TimescaleDB store as its
only data source and result sink (§10). Pre-publication: findings folded in; remaining
open items in §11.

This is the normative spec — rules, DDL, and canonical queries. The reasoning behind each
rule (alternatives considered, worked examples, and the full validation narrative) lives in
the companion [design rationale](forecast-store-rationale.md).

---

## 1. Overview

This document specifies a **forecast store**: a schema convention and accompanying SDK
behaviors that turn a Postgres/TimescaleDB database into the persistence, evaluation, and
monitoring layer for any forecasting system. It defines two storage patterns (a *belief
log* and a *forecast log*), a metadata layer that makes stores self-describing, a
generative rule for quantile columns, an enforcement model tuned for high-ingest
time-series workloads, and the canonical queries — vintage selection, leakage-safe feature
assembly, reproducible evaluation — that the convention exists to make correct and easy.

The convention runs on any Postgres 14+ and lights up additional capability (hypertables,
columnar compression) on TimescaleDB. It is model-agnostic: a time-series foundation model
behind an API, a HuggingFace checkpoint, an XGBoost pipeline, and a hand-rolled ARIMA all
write through the same tables.

---

## 2. Motivation

Producing a forecast has become nearly free — call a hosted model or load a checkpoint and
competitive zero-shot predictions come back in seconds. Everything *around* the forecast
has not gotten easier, and teams putting forecasting into production re-build the same
infrastructure, project after project:

- **Vintages.** A forecast made Monday for Wednesday and a forecast made Tuesday for
  Wednesday are different records, and both must be kept. Every serious deployment
  re-invents this two-timestamp schema by hand.
- **Point-in-time correctness.** Backtests and feature assembly must only use data that was
  known at forecast time. As-of joins are subtle to get right and silently wrong when you
  don't — the failure mode is leakage, which inflates backtest accuracy and evaporates in
  production.
- **Evaluation as an ongoing operation.** Metrics libraries exist, but their outputs die in
  notebooks. Accuracy should be a queryable, dashboardable time series tied to model and
  vintage.
- **Drift and anomaly monitoring.** Joining forecast vintages to realized actuals on a
  schedule is how you learn a model degraded — and the identical residual computation,
  pointed at an asset instead of a model, is textbook anomaly detection on physical
  telemetry. The missing layer is missing twice.

No product owns this layer. Model vendors stop at inference; warehouses stop at in-SQL
prediction; feature stores serve entity-keyed tabular ML and treat neither dense series nor
forecasts as first-class. The forecast store is that missing layer, specified as an open
convention so that the data outlives any particular tool.

---

## 3. Goals and non-goals

**Goals**

1. **One convention for production and research.** The same tables serve live serving,
   scheduled evaluation, and historical backtesting. Knowledge time is a writable domain
   attribute, so a backtest can simulate "what was known at the time" against the same
   columns production writes.
2. **Model- and compute-location-agnostic.** Forecasts may be produced by any model running
   anywhere (in-process, hosted API, Spark job, warehouse). The store is the layer all of
   them share.
3. **Leakage auditability.** Every forecast records the input window it was computed from;
   contamination is detectable with a one-line query.
4. **Portability with a gradient.** Plain Postgres 14+ suffices; TimescaleDB adds
   compression, retention, gap-filling, and rollups. No SQL:2011 temporal features are
   used (see §4.3).
5. **Self-describing stores.** Any client — Python SDK, TypeScript reader, BI tool, agent —
   can reconstruct a store's shape and guarantees from the store itself.
6. **Low write-path friction.** Defaults must not degrade ingest throughput; integrity
   mechanisms are chosen with time-series write volumes in mind (§8).

**Non-goals**

- Hosting or executing models. The store never runs inference.
- SQL as the model-integration surface. Integration happens in Python/TypeScript; SQL is
  the query surface for the stored results.
- Feature-store semantics (entity-keyed training-set assembly for tabular ML).
- General bi-temporal database machinery. The convention needs plain timestamp columns
  (three clocks, §4.1) and append-only semantics — nothing more.

---

## 4. Core concepts

### 4.1 Three clocks, append-only

Every record in the store is a **belief**: a value about some moment in time, held as of
some other moment in time, stored at a third. Forecast data is **tri-temporal** — in
temporal-database terms: valid time, decision time, transaction time. The knowledge clock
(the middle axis) is the one forecasting turns on; see the [design rationale](forecast-store-rationale.md)
§4.1 for why plain bi-temporal machinery doesn't cover it.

- `target_time` — what the belief is *about* (the delivery interval, the metered quarter
  hour). Always a `time_bucket` boundary at the series' declared resolution, so that
  forecasts, inputs, and actuals share a bucket grid and evaluation is a plain equi-join.
- `available_at` — when the belief was *knowable* in the domain: the moment the value
  became available to act on. A **domain claim, writable by design** — backfills set
  vendor publication times, backtests set simulated moments. One name, one axis, every
  table, so the canonical queries are table-parameterized with no column renaming.
  **Naming rule: a column may sound like system time only if the database stamps it** —
  `available_at` is a claim, `recorded_at` below is the measurement, and compute
  wall-clock (provenance trivia with no query role) goes in `runs.params` if a connector
  cares. (Why not `created_at`: rationale §4.1.)
- `recorded_at` — when the store learned it: **system time, never written by clients**,
  always `DEFAULT now()`. It is the measured fact that pins reproducible backtests (§9.2)
  and records each row's write mode (live vs. retro, §7.2) — retro-stamping is always
  visible, never undetectable. It sits in no primary key and needs no index; it must exist
  from day one because ingest times not captured at write time are unrecoverable. (Why
  `available_at` alone isn't enough: rationale §4.1.)

Tables are **append-only**: a new belief is a new row, never an `UPDATE`. This single rule
makes vintages, as-of queries, and reproducible evaluation fall out of the schema with no
triggers and no versioning machinery.

**One scoped exception — benchmark workspace.** Append-only governs *beliefs*: production
forecast history and actuals are never deleted or updated. Backtest artifacts, however,
are simulated beliefs written under a run label, and harness contracts (e.g. openstef-beam
"overwrite gracefully") may replace them wholesale — a delete scoped to that label, never
touching production history. Evaluations are beliefs about quality and stay append-only: a
re-evaluation is a new evaluation run (§7.5).

A **vintage** is the set of beliefs about a target that were current at a given knowledge
time. The **as-of query** (§9.1) selects one: latest `available_at` at or before a cutoff,
per target.

### 4.2 The two patterns

Everything in the store is an instance of one of two patterns:

**Pattern 1 — Belief log.** `(series_id, target_time, available_at, recorded_at, value…)`,
append-only. Instances: `actuals` (observations, possibly revised) and `predictors`
(externally produced forecast vintages, e.g. weather). Both are read by as-of vintage
selection. Single-belief actuals are the variant whose primary key admits one belief per
target (§6.1).

**Which instance does a stream belong in?** Every quantity has a **realization moment** —
the instant its true value becomes fixed: the meter interval ends, the auction clears.
Classify a stream by which side of that moment its rows are written on — equivalently, by
what a *newer row about the same target* would mean:

- Written **at or after realization**, a row reports a fact, and a fact can only be
  mis-measured — a newer row **corrects a measurement**: the stream is `actuals`. There is
  one truth; newer rows are better measurements of it.
- Written **before realization**, a row guesses at a value that does not exist yet — a
  newer row **re-predicts from newer information**: the stream is `predictors`. Each row
  was a valid forecast when made, not a mistake the next one fixes.

Publication *timing* plays no part in the test; what matters is where realization falls,
and it need not fall at `target_time` — e.g. a day-ahead power price is realized (and
becomes `actuals`) the day *before* its `target_time`, when the market clears, while a
desk's forecast of that same price is written beforehand and belongs in `predictors`
(full worked example: rationale §4.2). The full contrast between the two instances is §6.

**Pattern 2 — Forecast log.** A belief log *plus* run provenance — provenance is the
discriminator, not the columns. Instance: `forecasts` — beliefs written *with a run row*:
producers that can state their input window, parameters, and model identity, wherever the
compute ran (the SDK, a Spark job, another team's pipeline). A quantile band is an
*instance* declaration available to either pattern: forecast logs typically declare one,
and a probabilistic vendor feed may too (§6.2).

### 4.3 Why not SQL:2011 temporal features

The convention does not use Postgres 18/19's `WITHOUT OVERLAPS`/`AS OF` temporal
machinery: that feature models versioned state (one true row per key, revised in place),
covers only two of §4.1's three clocks, and cannot express the knowledge-time queries the
store depends on. The store does keep a system-time column (`recorded_at`, §4.1), but as a
plain queryable column — the frozen backtest needs both clocks in one predicate with
relative cutoffs (§9.2), which system-versioning syntax cannot express either. Plain
timestamp columns and append-only semantics do all the work, on any Postgres since 14. Full
reasoning: rationale §4.3.

### 4.4 A generative convention

Quantile requirements vary by store (one team forecasts a `[0.1, 0.5, 0.9]` band; a
benchmark uses seven levels; a risk desk wants `q2.5/q97.5`). Rather than one frozen DDL or
a lowest-common-denominator column set, the convention is **rules plus metadata plus a
generator**:

- The spec defines the *patterns*, the *naming rule*, and the *canonical queries*.
- Each store **declares** its configuration (quantile band, value columns, enforcement
  mode) as data, in the store itself (§5.2).
- The SDK/skill **generates** the concrete DDL and serving views from that declaration, at
  provisioning time. Changing a declaration is an explicit, tool-executed migration —
  never a side effect of a write.

A *reference instantiation* (shown throughout this document) is published as static DDL so
the convention remains copy-pasteable without any tooling.

---

## 5. Metadata layer

All tables live in a dedicated schema (default `forecast`, configurable).

### 5.1 The series registry

```sql
CREATE TABLE forecast.series (
    series_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            text COLLATE "C" UNIQUE NOT NULL,  -- the slug writers know
    sample_interval interval NOT NULL,    -- declared resolution; defines the bucket grid
    timezone        text,                 -- calendar-aware bucket alignment
    unit            text,
    description     text,                 -- for humans; nothing computes on it
    metadata        jsonb,                -- adapter/domain facts, documented keys (below)
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- strict resolver: raises on an unknown name → typos fail at write time
CREATE FUNCTION forecast.get_series_id(text) RETURNS bigint ... STABLE;
-- get-or-create: the auto-registration path (explicit metadata required, §8)
CREATE FUNCTION forecast.register_series(name text, ...) RETURNS bigint ...;
```

**Identity design.** Series identity is a generated `bigint` surrogate; the human-facing
slug lives in `series.name` (`fr_da_price`, `mvf_gorredijk_3`) — smaller, faster-sorting
keys on the store's largest tables and indexes (full comparison against a natural text
key: rationale §5.1). Three rules keep the surrogate safe and ergonomic:

1. **Resolve in SQL, at write time.** `forecast.get_series_id(name)` is strict — it raises
   on an unknown name, so a typo'd write fails loudly at insert time. Canonical usage stays
   a legible one-liner: `WHERE series_id = forecast.get_series_id('fr_da_price')`
   (`STABLE`, evaluated once by the planner). `register_series(...) RETURNS bigint` is the
   get-or-create path.
2. **Ids are never deleted, never reused.** Series are disabled via a flag, not removed. A
   name→id mapping, once correct, is therefore correct forever — bulk writers that must
   pre-resolve (COPY cannot evaluate functions: stage, then `INSERT … SELECT
   get_series_id(name), …`) cannot be misattributed by a stale cache.
3. **Reads speak names.** Generated read views join the registry to expose `name`, so BI
   tools, agents, and dashboards never traffic in bare numbers.

Because identity lives in the number, `name` can be renamed with a one-row update. One
deliberate asymmetry: `run_id` stays uuid, because runs are minted client-side by
distributed writers that cannot round-trip for an identity value; series are
registry-owned, which is exactly what makes a generated key workable.

**Typed-column criterion.** A column earns typed status only when store machinery computes
on it (`sample_interval`, `timezone`) or when it is a universal descriptor of the measure
itself (`unit`) — `description` is the one deliberate exception, kept typed as catalog
hygiene for humans, BI, and agents. Everything else — coordinates, country codes, capacity
limits, cohort labels — is still a series fact, but lives under adapter-documented keys in
`metadata` (e.g. `metadata->'location'->>'latitude'`, `metadata->'limits'->>'upper'`), so a
deployment outside a given domain carries no permanently-NULL imported columns. Promotion
is deliberately cheap — `series` is a small table, so lifting a key to a typed column is an
`ALTER TABLE` plus a version bump, taken when store machinery starts computing on it. (Why
tags beat a typed grouping column: rationale §5.1.)

The registry is **load-bearing, not descriptive**:

1. **Declared resolution.** The shared bucket grid (§4.1) is only enforceable if the
   resolution lives somewhere authoritative — the registry is the source of truth, and the
   SDK write path validates incoming timestamps against it.
2. **Adapters hydrate from it.** Feature engineering reads the registry, not just the
   points tables — e.g. an energy adapter derives weather joins and holiday calendars from
   `metadata->'location'`, and evaluation peak metrics read `metadata->'limits'`.

**Principle — runs snapshot; the registry stays current.** Registry rows are mutable
(limits get retuned, locations corrected). Reproducibility does not depend on registry
history because every run records the configuration it actually used (§7.1) — this keeps
slowly-changing-dimension machinery out of the core spec.

### 5.2 Store self-description

```sql
CREATE TABLE forecast.store_tables (
    table_name         text PRIMARY KEY,
    convention_version text NOT NULL,     -- per table: migrations move one table at a time
    config             jsonb NOT NULL,
    -- e.g. {"role": "forecasts",
    --       "quantile_band": [0.05, 0.1, 0.3, 0.5, 0.7, 0.9, 0.95],
    --       "has_mean": true,
    --       "enforcement": "monitor"}
    updated_at         timestamptz NOT NULL DEFAULT now()
);
```

One row per provisioned table. This is the store describing itself: the declared quantile
band, value columns, role, and enforcement mode are **data**, readable by every client — a
Python SDK, a TypeScript reader, an agent composing SQL, and an analyst in psql all
reconstruct the store's shape from the same rows instead of carrying config copies that
drift.

- **One row per table, not key-value.** A table's configuration is one logical object; a
  single-row upsert is atomic, so readers never observe a half-updated config.
- **Per-table `convention_version`.** Migrations move one table at a time, and every store
  records what provisioned it.
- **Mechanical keys vs. role.** `value_columns`, `knowledge_column`, and `has_runs` answer
  *how* to read and write the table; `role` answers *what the table means* — enumeration
  ("which tables are ground truth / vendor feeds / forecast logs"), the semantics of
  shared arithmetic (`recorded_at − available_at` means delivery lag, settlement lag, or
  write mode depending on role, §7.2), monitoring dispatch, and policy defaults including
  the scope of §4.1's sanctioned delete. The generator derives mechanical keys *from* the
  role, so declaration and DDL can't drift. (Why role isn't inferred from shape: rationale
  §5.2.)
- **`store_tables` is the read-routing registry; `series` carries no routing.** A series is
  a *quantity*: one identity may have measurements in `actuals`, vendor vintages in
  `predictors`, and forecasts in one or more forecast logs. So **APIs take table names**,
  and the reader resolves each named table's declaration here — an unknown table is a loud
  error, not SQL. This is also what makes additional instances (a second forecast table)
  readable with zero new machinery: another row here. (Full reasoning: rationale §5.2.)

---

## 6. Belief logs (pattern 1)

One shape, two instances, split by which side of the **realization moment** (§4.2) their
rows are written on. Every contract difference below follows from that:

| | `actuals` | `predictors` |
|---|---|---|
| A row is written | **after** the target realizes: "the value at T **was** x" — a measurement of an existing fact | **before**: "a vendor said the value at T **will be** x" — a guess at a value that does not exist yet |
| `available_at` | `DEFAULT now()` — a fact's knowledge time is when *you* learned it, and arrival is **measurable** (backfills state genuine claims instead) | `NOT NULL`, **no default** — a prediction's knowledge time is when *it was made* (its information set), which only the writer can **state** |
| Beliefs per target | one truth → one row (single-belief) or occasional revisions (revisioned) | many — one per publication cycle, as the information set refreshes |
| A superseded belief is | a corrected *measurement* — only the latest counts as truth; earlier rows survive as knowledge states (reproducibility pins, §9.2) | a distinct *prediction* from an older information set — not an error; each vintage stays individually scoreable (vendor skill by lead time) |
| Run provenance | none | none — the publication timestamp *is* the run key; a vendor model's context and params are unknowable |
| In evaluation | the **right** side of the join: the realized fact that beliefs are scored against | the **left**: pre-realization beliefs, scored once the target realizes (same machinery as our own forecasts, table-parameterized) |
| Monitoring (role) | missing-data alarms — the fact exists, the store lacks it; corrections are exceptional (a restatement may alert) | publication-lag watch — the cadence is the heartbeat (a missing cycle is a vendor incident) |
| Retention | kept — accuracy history is the moat | expirable after the evaluation window |

Which of `predictors` vs `forecasts` (pattern 2): **provenance, not origin** — a writer
that can supply a run row (context window, params, model identity) writes a forecast log,
wherever its compute runs; `predictors` is for vintages where all anyone knows is
`(target_time, available_at, value)`. Vendor feeds are the shorthand, not the rule — and
our own forecasts consumed as another model's input stay in `forecasts`, never copied here
(§9.1 reads them with the same as-of shape).

They are sibling tables, not one table with a role column: the evaluation join must read
"the actuals" without filtering out future-dated vendor rows, and retention is per-table.

### 6.1 Actuals

```sql
-- One shape; revisions are the primary-key switch:
CREATE TABLE forecast.actuals (
    series_id    bigint NOT NULL,
    target_time  timestamptz NOT NULL,
    available_at timestamptz NOT NULL DEFAULT now(),
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    value        double precision,
    PRIMARY KEY (series_id, target_time)                -- single-belief
 -- PRIMARY KEY (series_id, target_time, available_at)  -- revisioned
);
SELECT create_hypertable('forecast.actuals', 'target_time');
```

**Revisions are the PK switch.** Both shapes carry the same columns; what differs is the
key. **Single-belief** — `(series_id, target_time)` — admits one belief per target: for
owned telemetry and publish-once pipelines where a second belief is a defect to surface,
not a fact to record. Its canonical write is `INSERT … ON CONFLICT (series_id,
target_time) DO UPDATE SET value = EXCLUDED.value` under the generated `belief_guard`
trigger: an identical re-delivery is a silent no-op (first claim wins), a *conflicting*
value raises — never silently swallowed. (Verified inert under compression,
decompression, and DML over compressed chunks.) **Revisioned** — `(series_id, target_time,
available_at)` — keys revisions by the knowledge clock: for settlement-grade domains where
actuals are corrected for weeks and pinned cutoffs (§9.2) keep backtests reproducible. Its
idempotency is `ON CONFLICT DO NOTHING`: the revisioned PK names a belief's full
coordinates, so a colliding row with a different value is a **retcon** — refused,
first-wins — while a genuine correction is a new belief under a new `available_at`. A
publish-once *external* feed belongs in the revisioned shape, not single-belief: "never
revises" is a claim about the world, and a restatement should land and page rather than
bounce. (Why `available_at` defaults to `now()` here specifically, and what a claimless
backfill does: rationale §6.1.)

**Optional per-instance column: `target_time_observed`.** When ingest grid-aligns a 1:1
stream by snapping timestamps (§4.1), an actuals instance may declare
`target_time_observed timestamptz` — the device's original, unsnapped timestamp. It has no
query role (nothing as-ofs, joins, or pins on it) and exists for forensics and monitoring
only. **Nullable by design, never in the PK, never defaulted** — NULL means "not
recorded"; a NOT NULL contract would push mixed-quality feeds toward fabricating
`target_time` as the observation (rationale §6.1). `target_time_observed − target_time` is
the measurement-side jitter diagnostic, mirroring `recorded_at − available_at` as delivery
lag; the §8 sweep checks the in-bucket invariant. N:1 aggregation is different — it loses a
*set*, which no column holds; those inputs live upstream (§11).

An **existing-table mode** points the actuals role at a pre-existing telemetry hypertable
instead of provisioning one: `store_tables.config` records the source table and column
mapping, all read paths compile through it, and enforcement is necessarily `monitor` (§8).

### 6.2 Predictors

```sql
CREATE TABLE forecast.predictors (
    series_id    bigint NOT NULL,
    target_time  timestamptz NOT NULL,
    available_at timestamptz NOT NULL,    -- vendor publication time: stated, never defaulted
    recorded_at  timestamptz NOT NULL DEFAULT now(),  -- when the store ingested it
    value        double precision,
    PRIMARY KEY (series_id, target_time, available_at)
);
SELECT create_hypertable('forecast.predictors', 'target_time');
```

A probabilistic vendor declares a quantile band on its instance — `quantile_band` in its
`store_tables` config (§5.2) — same naming rule, same generated columns and reads as a
forecast log's band; what it never gains is run provenance. The point column is
**`value`, not `mean`** — a vendor's point value is often a deterministic run or a median,
so a `mean` column would assert a statistic the feed may not have (rationale §6.2). A
*known* statistic is per-feed registry metadata (`metadata->>'statistic'`).
`forecasts.mean` stays `mean`: pattern 2 is our own output, where the connector knows
exactly what it produced. `recorded_at − available_at` here is the measured **vendor
delivery lag** (§5.2).

---

## 7. The forecast log (pattern 2)

### 7.1 Runs

One row per inference call — who/what/when produced a forecast, from which inputs:

```sql
CREATE TABLE forecast.runs (
    run_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_name      text,                   -- caller-supplied grouping label (job, benchmark)
    model         text NOT NULL,
    model_version text,                   -- trained-artifact identity (e.g. MLflow version)
    available_at  timestamptz NOT NULL DEFAULT now(),  -- domain claim (writable)
    recorded_at   timestamptz NOT NULL DEFAULT now(),  -- system clock (never written)
    context_start timestamptz,
    context_end   timestamptz,
    started_at    timestamptz,            -- compute bounds: producer-measured claims
    finished_at   timestamptz,            -- (never defaulted; NULL = not measured)
    params        jsonb,                  -- resolved as-used config + gap-fill provenance
    CHECK (finished_at >= started_at)
);
```

- `context_start`/`context_end` record the input window actually queried, making leakage
  auditable in one line: any run where `context_end > available_at` is provably
  contaminated.
- `started_at`/`finished_at` bound the computation that produced this run's forecasts, as
  measured by the producer. Measurement provenance, not a fourth clock: no query role, never
  in a key, and never defaulted — a default `now()` would silently record *write* time as
  *compute* time. Training amortized across runs is excluded (a training event is a different
  entity). `finished_at − started_at` is inference latency; `recorded_at − finished_at` is the
  producer's own delivery lag — the same monitored quantity §6.2 defines for vendors, turned
  on ourselves. `finished_at` is not `available_at`: they coincide only under
  publish-on-compute live operation (where one clock read may honestly serve both). They
  diverge under gated release and post-compute pipelines (`available_at > finished_at` —
  the gap a measurable release delay), and in backtests/backfills, where `available_at` is
  simulated but `finished_at`, if stated, is real wall-clock.
- `params` is the **as-used snapshot**: the fully resolved configuration (registry values
  merged with the caller's engine config), plus gap-fill statistics from context assembly.
- `run_name` is a caller-supplied grouping label — job name, benchmark run, or model id —
  so evaluation can group runs without any job machinery in the store (§7.4).
- `model_version` identifies the trained artifact (e.g. an MLflow model version) — the
  thread from any stored forecast back to the exact model binary.
- `available_at` is writable by design: production stamps `now()`, backtests stamp the
  simulated decision moment. `recorded_at` guards the claim (§4.1): a production run has
  `available_at ≈ recorded_at`, a backtested run a claim in the past.

### 7.2 Forecast points

```sql
-- Reference instantiation: 7-level band + mean
CREATE TABLE forecast.forecasts (
    run_id       uuid NOT NULL,           -- no FK by default; see §8
    series_id    bigint NOT NULL,
    target_time  timestamptz NOT NULL,
    available_at timestamptz NOT NULL,    -- denormalized from the run (writable claim)
    recorded_at  timestamptz NOT NULL DEFAULT now(),  -- system clock (never written)
    mean double precision,
    q05  double precision, q10 double precision, q30 double precision,
    q50  double precision, q70 double precision, q90 double precision,
    q95  double precision,
    PRIMARY KEY (series_id, target_time, run_id)
);
SELECT create_hypertable('forecast.forecasts', 'target_time');
CREATE INDEX ON forecast.forecasts (series_id, target_time, available_at DESC);

-- Columnstore (generated for every points table; TimescaleDB only). The orderby
-- columns get minmax sparse indexes automatically — exactly the as-of predicates.
ALTER TABLE forecast.forecasts SET (timescaledb.enable_columnstore,
    timescaledb.segmentby = 'series_id',
    timescaledb.orderby   = 'target_time DESC, available_at DESC');
CALL add_columnstore_policy('forecast.forecasts',
    after => INTERVAL '7 days', if_not_exists => true);
```

- `available_at` is denormalized onto the points so as-of queries are single
  index-friendly scans, never joins through `runs`.
- Partitioning on `target_time`: evaluation and serving both join actuals on it, and
  compression/retention operate on old target periods.
- The table is append-only; a new belief is a new row.
- `recorded_at − available_at` is the row's **write mode** — *live* (a small gap — **write lag**:
  clock skew plus publish-to-persist latency) or *retro* (a past claim, written later) — making
  retro-writing visible, never undetectable. Write mode is not origin: a backtest vintage
  and a migrated slice of real production history are clock-identical; *origin*
  (production vs. simulation) lives once, on the run, as provenance — the points tables
  carry no `is_backtest` column. On live paths the gap is a monitored invariant with a
  deployment-declared threshold (§8). It joins delivery lag (§6.2) and jitter (§6.1) as
  the third clock-difference diagnostic.
- Columnstore settings follow pg-aiguide's hypertable guidance and are generated for every
  points table: `segmentby = 'series_id'`, `orderby` on the two clocks, a 7-day columnstore
  policy. (Why these specific settings: rationale §7.2.)
- The generator converts to a hypertable after creation (`create_hypertable` with
  `migrate_data`) rather than `CREATE TABLE … WITH (tsdb.*)`, so table DDL stays identical
  on every Postgres and re-running provision can upgrade a populated plain-Postgres store
  in place. (Why not the `WITH` form: rationale §7.2.)
- **One instance by default; more when a table-scoped policy genuinely differs**
  (retention split between production and experiment workspace, disjoint cadence domains,
  tenancy). Instances multiply with zero new read machinery — APIs take table names
  validated against `store_tables` (§5.2), so a second forecast table is readable the
  moment its row exists. (Full tradeoff discussion: rationale §7.2.)

### 7.3 Quantile representation

**Rule:** quantile values are wide, typed columns, one per level in the store's declared
band, plus a blessed `mean` column.

**Naming rule** (deterministic and bijective): column = `q` + the percent value, integer
part zero-padded to two digits, decimal point replaced by underscore. `0.05 → q05`, `0.5 →
q50`, `0.025 → q02_5`, `0.999 → q99_9`.

**The `mean` column** carries point/mean forecasts. A point forecast writes `mean`, never
`q50` — a mean is not a median, and the schema should not invite the lie.

**Band declaration and change.** The band is declared in `store_tables.config` (§5.2) and
is the source from which the generator emits columns, serving views, and per-column
compression settings. Changing the band is an explicit tool-executed migration with
documented cost. In practice bands change rarely: accuracy history is not comparable
across bands, so deployments pin one.

**Connector policy.** Model connectors meet the store's band: request it directly where
the model API allows, interpolate from the model's native quantiles where it does not, and
record which happened in `runs.params`.

Three alternatives — long/narrow rows, jsonb quantiles, one frozen wide superset — were
considered and rejected; see rationale §7.3.

### 7.4 Configuration: the three-way split

The configuration a forecasting engine needs splits three ways:

| Content | Home | Examples |
|---|---|---|
| Series facts — true regardless of how you forecast | `series` registry | resolution, unit, timezone, adapter metadata |
| Job definition — how this series is forecast | **caller-owned config** (code, YAML, orchestrator) | engine, model choice, horizons, quantiles, feature config |
| As-used record — what one run actually did | `runs.params` | resolved config snapshot, gap stats |

**Hydration.** Adapters build engine-native configuration *from* the store at run time:
series facts from the registry, the rest from the caller's engine config. If an engine
config explicitly contradicts the registry, that is an error, not a silent preference. The
caller's series bindings map engine roles to store series names (`{"target": "...",
"radiation": "..."}`), resolved via `get_series_id()` at hydration.

**Job definitions in the store are deferred past the MVP** — every job definition today is
caller-owned, and runs stand alone as the top of the provenance chain. See rationale §7.4
for the deferred `forecast.jobs` design.

### 7.5 Evaluation results

Evaluation outputs are stored relationally so accuracy is queryable history rather than
files. An accuracy metric **is a time series**: of the coordinates that identify a value —
which forecasts (`run_name`), which target (`series_id`), which as-of slice (`filtering`),
which window (`win`), which quantile, which metric — every one is constant along the time
axis; only the window position and the value vary. The store's doctrine for that shape
(§5.1) applies again: identity in a small dimension table with a bigint surrogate
(`evaluation_series`), points in a narrow fact table (`evaluation_metrics`) — the
series-table-plus-samples design proven by Prometheus-on-TimescaleDB. (Why a flat table
was rejected: rationale §7.5.)

```sql
-- One row per evaluation invocation: what was scored, how, against which data
CREATE TABLE forecast.evaluation_runs (
    eval_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_name    text NOT NULL,            -- the forecast group scored (matches runs.run_name)
    recorded_at timestamptz NOT NULL DEFAULT now(),
    params      jsonb                     -- as-used eval config: filterings, windows,
                                          -- metric providers, evaluation masks/period,
                                          -- and the frozen_at data pin (§9.2), applied
                                          -- to EVERY belief-log read of the evaluation
);

-- One row per accuracy series: the coordinates constant along the time axis
CREATE TABLE forecast.evaluation_series (
    eval_series_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_name  text NOT NULL,              -- keyed by name, NOT eval_run_id: a nightly
    series_id bigint NOT NULL,            --   monitor appends to stable series forever
    filtering text NOT NULL,              -- as-of spec: 'D-1T06:00' | lead time: 'PT36H'
    win       text NOT NULL,              -- evaluation window: '7D' | '21D' | 'global'
    quantile  text NOT NULL DEFAULT 'global',
    metric    text NOT NULL,              -- 'rMAE' | 'rCRPS' | 'pinball' | ...
    UNIQUE (run_name, series_id, filtering, win, quantile, metric)
);

-- One row per metric value: a plain, narrow time series
CREATE TABLE forecast.evaluation_metrics (
    eval_series_id bigint NOT NULL,
    ts             timestamptz NOT NULL,  -- window position: one row per rolling step;
                                          -- a single row spanning the period for 'global'
    eval_run_id    uuid NOT NULL,         -- provenance + re-evaluation version
    value          double precision,
    PRIMARY KEY (eval_series_id, ts, eval_run_id)
);
```

`evaluation_series` is keyed by `run_name`, not `eval_run_id`, so a nightly monitor appends
to the same rows for years — this works because `filtering` and `win` are **relative
specifications** (`D-1T06:00` is a per-delivery cutoff rule, `7D` a window size, never an
absolute moment), so the tuple is run-invariant by construction. Writers resolve ids
get-or-create (same resolver pattern as `get_series_id`, §5.1), canonicalizing the label
before lookup so near-duplicate spellings can't mint duplicate series.

**Placement rule:** what is constant across the *computation* (config, actuals pin,
`recorded_at`) lives on the run; what is constant along the *series* (the label tuple)
lives in the dimension; what varies point to point (`ts`, the belief version, the value)
lives in the facts. One report ⇄ one run row; one label tuple ⇄ one dimension row, shared
across runs. This is also where the §9.2 reproducibility pin lives: `evaluation_runs.params`
records `frozen_at`, so re-execution needs nothing outside the run row, and a
re-evaluation after settlement revisions is a new fact row under the same
`(eval_series_id, ts)` — accuracy history is itself a belief log, read by the same
latest-belief `DISTINCT ON` as everything else, with `eval_run_id` as the belief version.
(Full design rationale, including the three problems a flat table would have left
unsolved: rationale §7.5.)

Scheduled monitoring additionally materializes per-point errors into a `forecast_errors`
hypertable, rolled up by generated continuous aggregates, so dashboards read rollups
instead of re-running vintage joins.

---

## 8. Integrity and enforcement

The store depends on every point row referencing a registered series. The identity design
(§5.1) removes most of the risk before enforcement even enters — writers obtain ids
through the strict resolver, so an unknown or typo'd name errors at insert time. What
remains is a writer that invents or hardcodes a raw id, producing a point that is not
merely unindexed but *uninterpretable* (no resolution, no role).

**Why foreign keys are not the default.** Postgres FK checks are per-row triggers that
never batch — a real tax on bulk ingest — and at high write concurrency, shared locks on
the same few `series` rows drive MultiXact growth. For a convention aimed at high-ingest
time-series workloads, a default that degrades ingest is the wrong default. (Full
argument: rationale §8.)

**Default: `enforcement = "monitor"`.** Two layers, both on by default:

1. The SDK resolves names to ids through a client-side registry cache — effectively free,
   and safe to cache because ids are never reused (§5.1) — **mandatory** on every write
   path. Auto-registration on first write is permitted **only from explicitly declared
   metadata** (a dataset object that carries its resolution); bare frames error rather
   than having a resolution inferred from index spacing. Races resolve inside
   `register_series()` via `ON CONFLICT`.
2. An **orphan/grid sweep** ships with the TimescaleDB layer,
   `data_quality_sweep(scan_window)`: it scans the recent write window for ids absent from
   the registry, off-grid `target_time` (against the declared `sample_interval`), and —
   where declared — `target_time_observed` outside its target's bucket (§6.1). The sweep
   is **catalog-driven**: it discovers points tables from their `store_tables`
   declarations at execution time, so an instance added later — with tooling or by hand —
   is swept from the moment its declaration row exists, with no regeneration. Scheduling
   is deployment-owned (cron, `add_job`); alerts go through the standard hooks. The same
   sweep can flag anomalous `recorded_at − available_at` gaps — retro-stamped writes and
   unusually late arrivals.

Non-SDK writers (Spark jobs, dbt models, agents writing SQL) get the same write-time
prevention in their own language: `get_series_id()` (strict) and `register_series()`
(get-or-create) are plain SQL functions; the sweep remains as the backstop for writers
that bypass them with raw ids.

**Opt-in: `enforcement = "fk"`.** A deferrable FK (checks settle at commit, allowing
points-then-registration in one transaction), for deployments where it's rational:
low-volume forecast-only stores, regulated environments, dev/test.

**Opt-in: `append_only_guard`.** A generated `BEFORE UPDATE` trigger on revisioned points
tables that always raises — structural enforcement of §4.1's first law, for deployments
that want it in schema rather than convention. It costs nothing on the write path (no
legitimate path ever UPDATEs a points table) and is verified inert under compression,
decompression, and DML over compressed chunks. Single-belief actuals carry their
`belief_guard` (§6.1) unconditionally, which subsumes it.

**Existing-table mode** (§6.1) is necessarily `monitor`: the store cannot and should not
constrain a customer's pre-existing telemetry table.

Each table's mode is recorded in `store_tables.config` — the sweep derives its target list
from it, migration tooling consults it, and diagnostics can state which guarantees a given
store actually has.

---

## 9. Canonical queries

### 9.1 As-of (vintage selection)

*"What did we believe about tomorrow, as of 06:00 the day before?"*

```sql
SELECT DISTINCT ON (f.series_id, f.target_time)
       f.series_id, f.target_time, f.mean, f.q05, f.q50, f.q95, f.available_at
FROM   forecast.forecasts f
WHERE  f.series_id = forecast.get_series_id('mvf_gorredijk')
  AND  f.target_time >= '2024-07-30' AND f.target_time < '2024-07-31'
  AND  f.available_at <= '2024-07-29 06:00+02'   -- the as-of moment
ORDER  BY f.series_id, f.target_time, f.available_at DESC;
```

Wrapped as the helper `forecast_asof(series, target_range, asof)`. With `asof = now()` it
is "current best forecast" for serving; the generated `latest_<table>` view covers the hot
dashboard path. **The same query against `predictors` (on `available_at`) is
leakage-free feature assembly** — one query shape serves both serving and
point-in-time-correct input selection.

`DISTINCT ON` is used deliberately here rather than `last()`: over the mandated
`(series_id, target_time, available_at DESC)` index, it lets the planner choose between a
plain ordered scan (shallow revision depth) and TimescaleDB's SkipScan (deep revision
depth) with the same SQL and the same index. `last()` forecloses both — it's the
**required** form only where `DISTINCT ON` is unavailable: under a `GROUP BY`, as in
context assembly (§9.3). Full comparison: rationale §9.1.

### 9.2 Evaluation join

Score the forecast that *would have been used* — the cutoff is relative to the operational
decision deadline (e.g. day-ahead gate closure), which also makes rolling-origin
backtesting a plain `GROUP BY` over history:

```sql
WITH ef AS (   -- pick the vintage FIRST, then join (avoids a vintage-count blowup)
    SELECT DISTINCT ON (f.series_id, f.target_time)
           f.series_id, f.target_time, f.q50, f.q10, f.q90, f.run_id
    FROM   forecast.forecasts f
    WHERE  f.available_at <= f.target_time - interval '12 hours'  -- gate closure
      AND  f.recorded_at  <= :frozen_at   -- the pin guards forecasts too: backdated
    ORDER  BY f.series_id, f.target_time, f.available_at DESC     -- vintages are a
),                                                                -- supported write
ea AS (        -- revisioned actuals: latest belief, two clocks with two jobs
    SELECT DISTINCT ON (series_id, target_time) series_id, target_time, value
    FROM   forecast.actuals
    WHERE  available_at <= now()                  -- domain clock: what was knowable
      AND  recorded_at  <= :frozen_at             -- system clock: the dataset as the
    ORDER  BY series_id, target_time, available_at DESC   -- store held it at the pin
)
SELECT ef.series_id,
       time_bucket('1 day', ef.target_time) AS day,
       avg(abs(ef.q50 - ea.value))                              AS mae,
       avg((ea.value BETWEEN ef.q10 AND ef.q90)::int::float8)   AS coverage_80,
       avg(ef.q90 - ef.q10)                                     AS sharpness_80
FROM   ef JOIN ea USING (series_id, target_time)
GROUP  BY 1, 2;
```

The two kinds of cutoff do different jobs and both are needed for a **frozen backtest**:
the `available_at` cutoffs simulate what was knowable at each decision moment (domain
clock), while the `recorded_at` pin freezes the dataset as the store held it at one chosen
instant (system clock) — re-running with the same pin returns identical rows forever. The
pin must guard **every belief-log read in the query** — forecasts as well as actuals —
because `available_at` is a writable claim on both. The SDK captures one `frozen_at` at
evaluation start, uses it for all reads, and records it in `evaluation_runs.params`. For
live monitoring, set the pin to `now()` and the predicates are no-ops.

**Pin placement is the semantics** — four placements of the same predicate answer four
different questions:

| Placement | Pin | Answers |
|---|---|---|
| Unpinned | `now()` | Best current estimate of model skill — late backfills and settled revisions are visible, so results legitimately improve as history does. |
| Frozen | One pin, stamped at experiment start, recorded in `evaluation_runs.params` | Reproducible forever — the standard backtest. |
| Operational | A rolling pin (`recorded_at <= S` per simulated origin `S`, alongside `available_at <= S`) | How good the model was *as operated* — the `available_at` cutoff alone simulates what was theoretically knowable at `S`, but data knowable at `S` may not have reached the system until later. Over backfilled history this correctly degenerates: nothing was operationally present before its load date (fail loud, §6.1). |
| Forensic | One pin at one decision's timestamp, a single read | Reconstructs what the store held when a specific decision was made — separates a wrong model from inputs that were late or have since been revised. |

Per-quantile pinball-loss terms are enumerated per band column and **emitted by the
generator** from the declared band, e.g.
`avg(greatest(0.05*(ea.value-ef.q05), -0.95*(ea.value-ef.q05))) AS pinball_q05`; averaging
across the band approximates CRPS. Cross-quantile metrics (coverage, sharpness, crossing
checks) are plain row expressions — a structural benefit of the wide layout. Monitoring
variants use a `LEFT JOIN` with a configurable arrival-lag guard to distinguish "actual not
yet arrived" from "actual overdue for a realized period" (a data-quality alarm).

### 9.3 Context assembly

Model context windows must be one row per bucket, gaps explicit, ending at the last
complete bucket. The SDK issues `time_bucket_gapfill` over the relevant belief log, with
fill strategy per connector (`locf`, `interpolate`, or NaN passthrough) and a configurable
gap budget. On revisioned sources, a `DISTINCT ON … ORDER BY available_at DESC` CTE runs
first so gapfill regularizes the latest belief, not the revision stream — or, on
TimescaleDB with buckets at the series' native resolution, `locf(last(value,
available_at))` inside the gapfill aggregation expresses latest-belief inline and
collapses the two passes into one. The declared bounds become `context_start`/`context_end`
on the run, and gap statistics land in `runs.params`.

**Store-served covariates complete the leakage audit.** An engine-fed frame can only bound
the *target's* knowledge time (`context_end`); covariate publication times are absent from
a plain frame. When context assembly reads covariates from `predictors` instead, the
as-of cutoff makes the read leakage-free *by construction*, and the SDK records that
cutoff in `runs.params` (`covariates_asof`) — so the audit covers both halves of the
input: measured history via `context_end`, covariate vintages via the recorded cutoff.

---

## 10. Validation: the OpenSTEF spike

The convention was validated against [OpenSTEF](https://github.com/OpenSTEF/openstef) (LF
Energy's production short-term energy forecasting pipeline, deployed across thousands of
grid locations at the Dutch DSO Alliander) — chosen because its 4.x architecture is
bi-temporal in memory but has no storage layer of its own ("you own I/O"), making it a
realistic, externally-controlled stress test.

Adapters were built for every integration seam — production writes, store-served context
assembly, and OpenSTEF's own benchmark harness — and live-tested against a TimescaleDB
store on a real liander2024 wind park: 35,133 measurements with real per-row publication
claims, 591k weather vintage rows, and a 7-day Chronos-2 benchmark run (28 simulated
vintages, 8,092 forecast points) reproducing OpenSTEF's own evaluation exactly. Headline
result: rCRPS 0.0906, rMAE@q50 0.1248, calibration near-nominal across the band — computed
by a SQL query against `evaluation_series`/`evaluation_metrics`, i.e. §2's "accuracy as a
queryable time series," literally.

Key findings: OpenSTEF's production `predict()` output carries **no knowledge time at
all** — the store's write path is the missing recorder; its backtest requires **writable
knowledge time**, which SQL:2011 system time cannot serve (§4.3); and its own measurement
feed independently validates revisioned actuals (real ~48-hour settlement lags) and the
relative gate-closure cutoff this convention canonicalizes (§9.2).

Full narrative, numbers, and the adapter-level reference: rationale §10 and
[`integrations/openstef.md`](integrations/openstef.md).

---

## 11. Open questions

| Status | Item |
|---|---|
| closed | Auxiliary per-point statistics from engines (e.g. OpenSTEF's optional `stdev`) — **not persisted.** Nothing downstream of the forecaster consumes it; adapters record its presence in `runs.params` (`stdev_column_present`). Reopen only if a consumer appears. |
| closed | `evaluation_runs`/`evaluation_series`/`evaluation_metrics` shape — **validated**: metrics round-trip losslessly, subsets re-derive from stored backtest output + ground truth. Evidence: the OpenSTEF `EvaluationReport` round-trip ([integrations/openstef.md](integrations/openstef.md)). |
| open | Multi-producer / composed forecasts — ensembles and hierarchical compositions where several producers forecast the same target. Candidate: one run per producer + one for the composition, with member `run_id`s recorded in the composition run's `params`. Unexercised by the spike (single-model runs); decide when the first composed workflow is ported. |
| open | Period-valued targets (delivery products as `tstzrange`) — unexercised by OpenSTEF's point grid; low priority. |
| closed | Actuals unification — **resolved: columns unified; revisions are the PK switch** (2026-08-26). Both shapes carry the three clocks; `available_at` is the universal knowledge clock; single-belief writes are skip-or-raise via the generated `belief_guard` trigger; revisioned idempotency is retcon-refusal by doctrine. Convention 0.4.0. Full history: rationale §11. |
| open | Per-series revision-expectation contract — inside a shared revisioned table the sweep cannot tell an expected revision (settlement) from an incident (a publish-once feed restating); routing that alarm needs a per-series declaration (sketch: `series.metadata->'contract'->>'single_belief'`). Decide when the first publish-once external feed is ingested. |
| open | TimescaleDB-required posture — direction set (2026-08-26): the convention will require TimescaleDB rather than treat it as an optional acceleration layer. When this lands, revisit §1/§4.3's any-Postgres-14+ language. |
| open | Continuous aggregates — **none generated yet; two sanctioned homes** (model-input downsampling over single-belief series; evaluation rollups), plus rules for what stays out (caggs over revision-bearing or forecast tables, and any pinned surface). Full design: rationale §11. |
| open | Connector band-mapping statistics (requested vs. interpolated quantiles) — one spec paragraph + `params` provenance flag. The OpenSTEF path sidesteps it (workflow configs request the band directly); needed when the first interpolating connector lands. |
| open | `forecast.jobs` control-plane table — deliberately deferred past the MVP (§7.4): its only consumers (worker, data-arrival triggers, agent-managed pipelines) are later-stage. Arrives as a new table plus a nullable `runs.job_id` column; nothing downstream of `runs` changes. |
| closed | Multi-instance roles — **shipped.** `StoreConfig.with_tables(...)` declares additional instances; the generator emits each extra table identically to a canonical one from one instance-plan source. Live-verified additively on the provisioned store. |
| closed | Chunk skipping — **removed** (2026-08-25) after issues surfaced with the early-access feature (≥ 2.16). Knowledge-clock predicates rely on `target_time` partition pruning plus orderby minmax sparse indexes; revisit if the feature matures to GA. |
| open | Per-series expected arrival lag — deliberately cut from the MVP as a declared value. Arrival is *measured* everywhere (`recorded_at`); calibrate any future freshness alerting from observed gaps rather than declaring one. |

Decisions already closed in this draft: quantile representation (wide, generative,
band-in-metadata — §7.3); point/mean forecasts (`mean` column — §7.3); versioned inputs as
a second belief-log instance (§6.2); uniform `available_at` vocabulary (§4.1); the metadata
layer (§5); enforcement defaults (monitor-first, FK opt-in — §8); namespacing (dedicated
`forecast` schema — §5); configuration split (registry / caller config / runs, with the
in-store jobs table deferred past the MVP — §7.4); series identity (bigint surrogate +
strict SQL resolver, names in the registry — §5.1); no `kind` column — a series is a
quantity, tables are belief types about it, and read APIs take table names resolved
against `store_tables` (§5.2); predictor point values as `value`, with the statistic in
registry metadata (§6.2); the forecasts-vs-predictors decision test as provenance, not
origin (§6.2).
