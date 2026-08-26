# The Forecast Store Convention

**Design document — draft v0.4** · 2026-08-26 · `forecast-store`
Status: **spike-validated.** The design below has been implemented (generator, skill, SDK
read/write paths, OpenSTEF adapters) and validated live against OpenSTEF 4.x — including
the liander2024 Chronos-2 benchmark running end-to-end with a TimescaleDB store as its
only data source and result sink (§10). Pre-publication: findings folded in; remaining
open items in §11.

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
columnar compression) on TimescaleDB. It is model-agnostic: a
time-series foundation model behind an API, a HuggingFace checkpoint, an XGBoost pipeline,
and a hand-rolled ARIMA all write through the same tables.

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
temporal-database terms: valid time, decision time, transaction time. Bi-temporal
machinery standardizes the first and last; the middle axis, the knowledge clock, is the
one forecasting turns on.

- `target_time` — what the belief is *about* (the delivery interval, the metered quarter
  hour). Always a `time_bucket` boundary at the series' declared resolution, so that
  forecasts, inputs, and actuals share a bucket grid and evaluation is a plain equi-join.
- `available_at` — when the belief was *knowable* in the domain: the moment the value
  became available to act on. A **domain claim, writable by design** — backfills set
  vendor publication times, backtests set simulated moments. One name, one axis, every
  table — our forecasts, external inputs, revisioned actuals — so the canonical queries
  are table-parameterized with no column renaming. The name is chosen against a common
  trap: hand-rolled schemas call this column `created_at`, which *sounds like* system
  time — a machine-stamped physical fact — while the column must accept written pasts
  (backtests, bulk loads). In live operation compute time, availability, and ingest
  coincide, so the misnomer hides; the first backtest forces a value that is false under
  one reading of "created" or breaks vintages under the other. The convention's naming
  rule: **a column may sound like system time only if the database stamps it** —
  availability is a claim, `recorded_at` below is the measurement, and compute wall-clock
  (provenance trivia with no query role) goes in `runs.params` if a connector cares.
- `recorded_at` — when the store learned it: **system time, never written by clients**,
  always `DEFAULT now()`. Because `available_at` is a writable claim, it cannot by itself
  make backtests trustworthy: a row inserted at 09:00 stamped `available_at = 06:00`
  would let a backtest "see" data production never had, and honest late arrivals silently
  change past backtest results for the same cutoff. `recorded_at` is the measured fact
  that resolves both — it pins reproducible backtests (§9.2) and records each row's
  write mode (live vs retro, §7.2): retro-stamping is always visible, never undetectable. It sits in no primary key and
  needs no index; it must exist from day one because ingest times not captured at write
  time are unrecoverable.

Tables are **append-only**: a new belief is a new row, never an `UPDATE`. This single rule
makes vintages, as-of queries, and reproducible evaluation fall out of the schema with no
triggers and no versioning machinery.

**One scoped exception — benchmark workspace.** Append-only governs *beliefs*: production
forecast history and actuals are never deleted or updated. Backtest artifacts, however,
are simulated beliefs written under a run label, and harness contracts (e.g. openstef-beam
"overwrite gracefully") may replace them wholesale — a delete scoped to that label, never
touching production history. Evaluations are beliefs about quality and stay append-only:
a re-evaluation is a new evaluation run (§7.5).

A **vintage** is the set of beliefs about a target that were current at a given knowledge
time. The **as-of query** (§9.1) selects one: latest `available_at` at or before a cutoff,
per target.

### 4.2 The two patterns

Everything in the store is an instance of one of two patterns:

**Pattern 1 — Belief log.** `(series_id, target_time, available_at, recorded_at, value…)`,
append-only. Instances: `actuals` (observations, possibly revised) and `predictors`
(externally produced forecast vintages, e.g. weather). Both are read by as-of vintage
selection. Single-belief actuals are the variant whose primary key admits one belief
per target (§6.1).

**Which instance does a stream belong in?** Every quantity has a **realization moment** —
the instant its true value becomes fixed: the meter interval ends, the auction clears.
Classify a stream by which side of that moment its rows are written on — equivalently, by
what a *newer row about the same target* would mean:

- Written **at or after realization**, a row reports a fact, and a fact can only be
  mis-measured — a newer row **corrects a measurement** (market settlement restates a
  meter reading): the stream is `actuals`. There is one truth; newer rows are better
  measurements of it.
- Written **before realization**, a row guesses at a value that does not exist yet — a
  newer row **re-predicts from newer information** (the 12Z weather run replacing the
  06Z run): the stream is `predictors`. Each row was a valid forecast when made, not a
  mistake the next one fixes.

Publication *timing* plays no part in the test; what matters is where realization falls,
and it need not fall at `target_time`. Concretely: the day-ahead price for Wednesday
18:00 comes into existence on *Tuesday* — order books close at 12:00 CET, the clearing
algorithm crosses aggregate supply and demand, and one binding price per delivery period
publishes ~12:45. Before that run there is no price, only forecasts of it; after it there
is a fact that only an exchange error could restate. The auctioneer does not predict the
price — the clearing computation creates it ("day-ahead" names the product, not a guess
about the day ahead). Exchange publications are therefore `actuals` with `available_at` a
day before `target_time`, while a desk's Monday forecast *of* the same price is written
before the value exists and belongs in `predictors`, scoreable against the cleared price
once it exists. The full contrast between the two instances is §6.

**Pattern 2 — Forecast log.** A belief log *plus* run provenance — provenance is the
discriminator, not the columns. Instance: `forecasts` — beliefs written *with a run row*:
producers that can state their input window, parameters, and model identity, wherever the
compute ran (the SDK, a Spark job, another team's pipeline). A quantile band is an
*instance* declaration available to either pattern: forecast logs typically declare one,
and a probabilistic vendor feed may too (§6.2).

### 4.3 Why not SQL:2011 temporal features

Postgres 18/19's temporal machinery models **versioned state**: one true row per key,
revised in place, system time as an audit trail the system stamps. It also spans only two
of §4.1's three axes — valid and transaction time; the knowledge clock has no
standardized counterpart. The forecast store is the opposite: an immutable belief *log* where many vintages per target coexist as
first-class data, and where knowledge time must be **writable** — bulk loads and simulated
backtests set it explicitly. `WITHOUT OVERLAPS` on a (series, target) key would forbid
vintages outright, and `AS OF` syntax cannot express the workhorse query
(argmax-over-knowledge-time with relative cutoffs). The store does keep a system-time
column (`recorded_at`, §4.1) — but as a plain queryable column, because the frozen
backtest needs *both* clocks in one predicate with relative cutoffs (§9.2), which
system-versioning machinery cannot express either. Plain timestamp columns and
append-only semantics do all the work, on any Postgres since 14.

### 4.4 A generative convention

Quantile requirements vary by store (one team forecasts a `[0.1, 0.5, 0.9]` band; a
benchmark uses seven levels; a risk desk wants `q2.5/q97.5`). Rather than one frozen DDL or
a lowest-common-denominator column set, the convention is **rules plus metadata plus a
generator**:

- The spec defines the *patterns*, the *naming rule*, and the *canonical queries*.
- Each store **declares** its configuration (quantile band, value columns, enforcement
  mode) as data, in the store itself (§5.2).
- The SDK/skill **generates** the concrete DDL and serving views
  from that declaration, at provisioning time. Changing a declaration is an explicit,
  tool-executed migration — never a side effect of a write.

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
slug lives in `series.name` (`fr_da_price`, `mvf_gorredijk_3`). Points tables carry only
the bigint — 8 bytes instead of a 20-odd-byte string in every row and, more importantly,
in every entry of the PK and as-of indexes, which never compress. Integer comparisons also
cut the sort cost of the `DISTINCT ON` paths. Three rules make the surrogate safe and
ergonomic:

1. **Resolve in SQL, at write time.** `forecast.get_series_id(name)` is strict — it raises
   on an unknown name, so a typo'd write fails loudly at insert time, stronger than any
   after-the-fact sweep. Canonical usage stays a legible one-liner:
   `WHERE series_id = forecast.get_series_id('fr_da_price')` (`STABLE`, evaluated once by
   the planner). `register_series(...) RETURNS bigint` is the get-or-create path.
2. **Ids are never deleted, never reused.** Series are disabled via a flag, not removed. A
   name→id mapping, once correct, is therefore correct forever — bulk writers that must
   pre-resolve (COPY cannot evaluate functions: stage, then
   `INSERT … SELECT get_series_id(name), …`) cannot be misattributed by a stale cache.
3. **Reads speak names.** Generated read views join the registry to expose `name`, so BI
   tools, agents, and dashboards never traffic in bare numbers.

Because identity lives in the number, `name` can be renamed with a one-row update — keep
names stable as a courtesy, not a constraint. One deliberate asymmetry: `run_id` stays
uuid, because runs are minted client-side by distributed writers that cannot round-trip
for an identity value; series are registry-owned, which is exactly what makes a generated
key workable. *Alternative considered — natural text key* (`series_id text` in every
table): join-free queries and self-describing rows in psql, but permanently larger
never-compressed index entries and collated sorts on the store's biggest tables — and the
strict resolver reverses the integrity comparison anyway: a typo'd name now errors at
write time, where the text design let it create a phantom series for the sweep to find
later.

**Typed-column criterion.** A column earns typed status only when store machinery computes
on it (`sample_interval`, `timezone`) or when it is a universal descriptor of the
measure itself (`unit`). One acknowledged exception: `description`, a documentation field
for humans, BI, and agents — nothing computes on it, and the spec keeps it typed anyway as
catalog hygiene. Everything else — coordinates, country codes, capacity limits, cohort
labels — is still a series fact and still lives in the registry, but under
**adapter-documented keys in `metadata`**
(e.g. `metadata->'location'->>'latitude'`, `metadata->'limits'->>'upper'`,
`metadata->'tags'->>'group'`), so that a deployment outside a given domain carries no
permanently-NULL columns imported from someone else's use case. Tags in particular beat a
typed grouping column: real fleets segment along several axes at once (region × asset type
× customer), which one flat column cannot express and arbitrary tag keys can — and grouped
rollups work fine on a jsonb extraction. Promotion is deliberately cheap: `series` is a
small plain table, so lifting a key to a typed column is an `ALTER TABLE` plus a
convention-version bump, taken when store machinery starts computing on it (the expected
example: alarm thresholds when residual monitoring ships).

The registry is **load-bearing, not descriptive**:

1. **Declared resolution.** The shared bucket grid (§4.1) is only enforceable if the
   resolution lives somewhere authoritative. Client-side containers (pandas `attrs`,
   dataframe metadata) are silently dropped by common operations and libraries fall back to
   silent defaults; the registry is the source of truth, and the SDK write path validates
   incoming timestamps against it.
2. **Adapters hydrate from it.** Feature engineering reads the registry, not just the
   points tables — e.g. an energy adapter derives weather joins and holiday calendars from
   `metadata->'location'` and `->>'country_code'`, and evaluation peak metrics read
   `metadata->'limits'`. The same limits become per-series alarm thresholds when residual
   monitoring points the evaluation machinery at assets instead of models.

**Principle — runs snapshot; the registry stays current.** Registry rows are mutable
(limits get retuned, locations corrected). Reproducibility does not depend on registry
history because every run records the configuration it actually used (§7.1). This keeps
slowly-changing-dimension machinery out of the core spec; system-versioned metadata remains
an optional future enhancement, not a correctness requirement.

### 5.2 Store self-description

```sql
CREATE TABLE forecast.store_tables (
    table_name         text PRIMARY KEY,
    convention_version text NOT NULL,     -- per table: migrations move one table at a time
    config             jsonb NOT NULL,
    -- e.g. {"role": "own_forecasts",
    --       "quantile_band": [0.05, 0.1, 0.3, 0.5, 0.7, 0.9, 0.95],
    --       "has_mean": true,
    --       "enforcement": "monitor"}
    updated_at         timestamptz NOT NULL DEFAULT now()
);
```

One row per provisioned table. This is the store describing itself: the declared quantile
band, value columns, role, and enforcement mode are **data**, readable by every client. A
Python SDK in an orchestrator, a TypeScript reader in a dashboard, an agent composing SQL,
and an analyst in psql all reconstruct the store's shape from the same rows instead of
carrying config copies that drift.

Design notes:

- **One row per table, not key-value.** A table's configuration is one logical object;
  a single-row upsert is atomic, so readers never observe a half-updated config.
- **Per-table `convention_version`.** Migrations move one table at a time; "which version
  is *this* table at" is the question migration tooling asks. It is also the operable form
  of a published stability commitment: every store records what provisioned it.
- **Two layers in each declaration: mechanical keys and role.** `value_columns`,
  `knowledge_column`, and `has_runs` answer *how* to read and write the table — code
  dispatches on them per operation. `role` answers *what the table means*, and its
  consumers are purpose-level: enumeration ("which tables are ground truth / vendor feeds
  / forecast logs" — the only way to ask under multi-instance without hardcoding names),
  semantics of shared arithmetic (`recorded_at − available_at` is vendor delivery lag on
  predictors, settlement lag on actuals, write lag / write mode on forecast logs, §7.2), monitoring dispatch (missing-data alarms watch actuals; publication-lag watches
  predictors; drift watches forecast logs — a misrouted role is a misrouted page, not a
  mislabel), and policy defaults including the scope of §4.1's sanctioned delete. Role is
  not inferred from shape — declaration beats introspection — and it cannot drift: the
  generator derives the mechanical keys *from* the role, so declaration and DDL share one
  source.
- **`store_tables` is the read-routing registry; `series` carries no routing at all.**
  A series is a *quantity*: one identity may have measurements in `actuals`, vendor
  vintages in `predictors`, and forecasts in one or more forecast logs — three belief
  types about the same thing (which is what makes vendor scoring a same-series
  equi-join). So routing cannot be a series attribute; **APIs take table names**, and the
  reader resolves each named table's declaration here — `value_columns`,
  `knowledge_column`, `has_runs` — validating the request against the store's own
  self-description (names are whitelisted by construction: an unknown table is a loud
  error, not SQL). The §4.2 classification rule remains, reworded to what it always was:
  a rule about **which table a writer sends a stream to**, not a registration. This is
  also what makes additional instances (a second forecast table) readable with zero new
  machinery: another row here.

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
our own forecasts consumed as another model's input stay in `forecasts`, never copied
here (§9.1 reads them with the same as-of shape).

They are sibling tables, not one table with a role column: the evaluation join must read
"the actuals" without filtering out future-dated vendor rows, and retention is
per-table.

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

**Revisions are the PK switch.** Both shapes carry the same columns: `available_at` is the
knowledge clock everywhere, its `DEFAULT now()` making the lazy path honest (arrival)
and its writability making backfills honest on *both* tiers — state genuine per-row
availability and history behaves as if ingested live; a claimless load defaults to its
load date, and knowledge-cutoff backtests over it fail loud, not quietly optimistic.
What differs is the key. **Single-belief** — `(series_id, target_time)` — admits one
belief per target: for owned telemetry and publish-once pipelines where a second belief is a defect
to surface, not a fact to record. Its canonical write is `INSERT … ON CONFLICT
(series_id, target_time) DO UPDATE SET value = EXCLUDED.value` under the generated
`belief_guard` trigger: an identical re-delivery is a silent no-op (the stored claim is
preserved — first claim wins), a *conflicting* value raises — never silently swallowed.
(The trigger is verified inert under compression, decompression, policy jobs, and DML
over compressed chunks; deep re-delivery over compressed history decompresses touched
segments — the workspace GUC precedent, §10, applies.) **Revisioned** — `(series_id,
target_time, available_at)` — keys revisions by the knowledge clock: for
settlement-grade domains where actuals are corrected for weeks and pinned cutoffs (§9.2)
keep backtests reproducible, `recorded_at` guarding the claims. Its idempotency is `ON
CONFLICT DO NOTHING`, and that is doctrine, not accident: the revisioned PK names a
belief's *full coordinates*, so a colliding row with a different value is a **retcon** —
a re-assertion of what a past publication said — refused first-wins; a genuine
correction is a new belief under a new `available_at`. A publish-once *external* feed
belongs in the revisioned shape, not single-belief: "never revises" is a claim about the world, the world
restates, and a restatement should land and page rather than bounce.

**Optional per-instance column: `target_time_observed`.** When ingest grid-aligns a 1:1
stream by snapping timestamps (§4.1), an actuals instance may declare
`target_time_observed timestamptz` — the device's original, unsnapped timestamp. It is
the *subject* clock as observed, not a fourth epistemic clock: a writable claim with no
query role (nothing as-ofs, joins, or pins on it), carried for forensics and monitoring.
**Nullable by design** — NULL honestly means "not recorded"; a NOT NULL contract would
push mixed-quality feeds toward fabricating `target_time` as the observation, jitter-zero
lies that poison the diagnostic (the lazy path must stay the safe path). Never in the PK,
never defaulted. `target_time_observed − target_time` is the measurement-side jitter
diagnostic, mirroring `recorded_at − available_at` as delivery lag; the §8 sweep checks
the in-bucket invariant (an observed timestamp outside its target's bucket is a snapping
bug) and can watch per-series NULL coverage. N:1 aggregation is different — it loses a
*set*, which no column holds; those inputs live upstream (§11).

An **existing-table mode** points the actuals role at a pre-existing telemetry hypertable
instead of provisioning one: `store_tables.config` records the source table and column
mapping (including the registry join to series ids), all read paths compile through it,
and enforcement is necessarily `monitor` (§8).

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
`store_tables` config (§5.2) — same naming rule, same generated columns and
reads as a forecast log's band; what it never gains is run provenance. One more choice
beyond the table above: the point column is **`value`, not `mean`** — a
vendor's point value is often a deterministic run or a median, so a `mean` column would
assert a statistic the feed may not have (the `mean`≠`q50` honesty rule in reverse). A
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
    params        jsonb                   -- resolved as-used config + gap-fill provenance
);
```

- `context_start`/`context_end` record the input window actually queried, making leakage
  auditable in one line: any run where `context_end > available_at` is provably
  contaminated.
- `params` is the **as-used snapshot**: the fully resolved configuration (registry values
  merged with the caller's engine config), plus gap-fill statistics from context assembly.
  Snapshots are a few KB of mostly identical jsonb per run and compress to near nothing; a
  deduplicating config-versions table is a documented scale path, not part of v0.
- `run_name` is a caller-supplied grouping label — the job name, benchmark run, or model
  id — so evaluation can group runs without any job machinery in the store (§7.4).
- `model_version` identifies the trained artifact (e.g. the MLflow model version) — the
  thread from any stored forecast back to the exact model binary.
- `available_at` is writable by design: production stamps `now()`; backtests stamp
  the simulated decision moment. Same column, both uses (§4.3). `recorded_at` guards the
  claim (§4.1): a production run has `available_at ≈ recorded_at`, a backtested run a
  claim in the past — so run-level provenance is self-contained, without inspecting
  points.

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

Design notes carried in the DDL:

- `available_at` is denormalized onto the points so as-of queries are single
  index-friendly scans, never joins through `runs`.
- Partitioning on `target_time`: evaluation and serving both join actuals on it, and
  compression/retention then operate on old target periods.
- The table is append-only; a new belief is a new row.
- `recorded_at` makes retro-writing **visible, never undetectable**: the gap
  `recorded_at − available_at` is the row's **write mode** — *live* (gap ≈ **write
  lag**: clock skew plus publish-to-persist latency, milliseconds in-process,
  structurally more through queues) or *retro* (a past claim, written later). Write mode
  is not origin: a backtest vintage and a migrated slice of real production history are
  clock-identical, and both are legitimate — *origin* (production vs simulation) lives
  once, on the run, as provenance; the points tables carry no `is_backtest` column
  because that fact is run-level, not row-level. The gap cannot be constrained (its
  freedom is what admits backtests and migrations at all); on live paths it is a
  monitored invariant with a deployment-declared threshold (§8's anomalous-gap alert).
  Write lag joins delivery lag (`recorded_at − available_at` on predictors, §6.2) and
  jitter (`target_time_observed − target_time`, §6.1) as the third clock-difference
  diagnostic: same subtraction, different table, different alarm.
- Columnstore settings follow pg-aiguide's hypertable guidance and are generated for
  every points table: `segmentby = 'series_id'` (the primary filter, high row density per
  chunk); `orderby` on the two clocks (adjacent rows in a segment are a target's
  consecutive vintages — a natural progression that compresses well — and orderby columns
  get minmax sparse indexes automatically, which are exactly the as-of predicates); a
  7-day columnstore policy (beliefs are immutable on write, but the recent window stays
  rowstore for the hot serving path). Knowledge-clock predicates — as-of cutoffs, publication windows — are
  served by partition pruning on `target_time` plus the orderby minmax sparse indexes
  within each chunk.
- The generator deliberately converts after creation (`create_hypertable` with
  `migrate_data`) rather than using `CREATE TABLE … WITH (tsdb.*)`: the table DDL stays
  identical on every Postgres (one canonical schema), and re-running provision upgrades a
  populated plain-Postgres store in place when the extension appears — the WITH form
  cannot retrofit, since `IF NOT EXISTS` no-ops on an existing plain table.
- **One instance by default; more when a table-scoped policy genuinely differs.** The
  forecast log may be instantiated more than once (the pattern is defined once and
  instantiated per role, and `store_tables` admits multiple rows sharing a role). The
  legitimate drivers are all per-hypertable policies: retention split between production
  history and experiment workspace (the Decision-2 sibling argument applied within the
  role), chunk intervals or bands for genuinely disjoint cadence domains, and tenancy
  (erasure as `DROP TABLE`). The cost is what the single table buys — cross-model
  evaluation as one query, one serving view, one as-of surface — so the default stays
  one, and splitting requires a policy that actually differs. Instances multiply with
  zero new read machinery: APIs take table names validated against `store_tables`
  (§5.2), so a second forecast table is readable the moment its row exists — an extra
  instance is just one more declared `store_tables` row the generator emits (own
  band or PK shape per instance; provisioning is additive). The workspace driver is validated
  in the OpenSTEF benchmark harness (§10).

### 7.3 Quantile representation

**Rule:** quantile values are wide, typed columns, one per level in the store's declared
band, plus a blessed `mean` column.

**Naming rule** (deterministic and bijective): column = `q` + the percent value, integer
part zero-padded to two digits, decimal point replaced by underscore.
`0.05 → q05`, `0.5 → q50`, `0.025 → q02_5`, `0.999 → q99_9`.

**The `mean` column** carries point/mean forecasts. A point forecast writes `mean`, never
`q50` — a mean is not a median, and the schema should not invite the lie.

**Band declaration and change.** The band is declared in `store_tables.config`
(§5.2) and is the source from which the generator emits columns,
serving views, and per-column compression settings. Changing the band is an explicit
tool-executed migration (add columns, regenerate derived objects) with documented cost.
In practice bands change rarely: accuracy history is not comparable across bands, so
deployments pin one.

**Connector policy.** Model connectors meet the store's band: request it directly where
the model API allows, interpolate from the model's native quantiles where it does not, and
record which happened in `runs.params`.

**Alternatives considered:**

- *Long/narrow* (one row per quantile, `quantile` as a numeric column). Gives one
  permanently stable DDL and quantile-generic per-quantile rollups (`GROUP BY quantile`),
  but: each value pays the full key tuple (~70–90 bytes of key and row header per 8-byte
  value, and ~band-size× the rows and index entries — indexes never compress); the hot
  serving path needs a pivot on every read; a logical forecast becomes multiple rows,
  admitting torn partial writes; and the genericity argument fails exactly where it
  matters most — cross-quantile metrics (interval coverage, sharpness, quantile-crossing
  checks) need multiple levels of the same point in one expression, which wide rows give
  for free.
- *jsonb quantiles* (`{"0.05": 11.2, …}` per row). Flexibility of long with the row count
  of wide, but: several-fold worse compression on the store's largest table (keys repeated
  in every row, values as opaque blobs, no per-column chunk exclusion); an extraction and
  cast in every canonical query; a text-key normalization footgun (`'0.5'` vs `'0.50'` vs
  `'q50'`) that moves errors from write time to silent read-time NULLs; hostile to BI and
  to per-quantile aggregation. The principled line: jsonb for run-scoped metadata that is
  occasionally filtered (`runs.params` — correct use), typed columns for point-scoped
  measures that everything aggregates.
- *One frozen wide superset* (bless N columns forever). Any fixed set eventually fails —
  `q2.5/q97.5`, the standard 95% interval, is already awkward — and every miss is a spec
  break in a document meant to be cited as stable. The generative convention (§4.4) keeps
  wide's physics without freezing the set.

### 7.4 Configuration: the three-way split

The configuration a forecasting engine needs splits three ways:

| Content | Home | Examples |
|---|---|---|
| Series facts — true regardless of how you forecast | `series` registry | resolution, unit, timezone, adapter metadata |
| Job definition — how this series is forecast | **caller-owned config** (code, YAML, orchestrator) | engine, model choice, horizons, quantiles, feature config |
| As-used record — what one run actually did | `runs.params` | resolved config snapshot, gap stats |

**Hydration.** Adapters build engine-native configuration *from* the store at run time:
series facts from the registry, the rest from the caller's engine config. One source of
truth; if an engine config explicitly contradicts the registry, that is an error, not a
silent preference. The caller's series bindings map engine roles to store series names
(`{"target": "...", "radiation": "..."}`), resolved via `get_series_id()` at hydration —
they double as the rename map between store series and the column names the engine
expects.

**Job definitions in the store — deferred.** The middle row of the table has an obvious
database home: a `forecast.jobs` table (engine, series bindings, horizon, quantiles,
trigger spec, engine config) that would make the store a control plane — a standalone
worker polls it, an orchestrator task becomes `run_job(job_id)`, an agent creates a
pipeline by inserting a row, and job registration validates quantiles ⊆ band and series
bindings before any run executes. That table is deliberately **not** part of the MVP: its
only consumers (the Forecaster worker, data-arrival triggers, agent-managed pipelines) are
later-stage, and in the MVP every job definition is caller-owned — the deployment
example's settings object, a benchmark script's constants, an Airflow DAG's parameters.
Runs therefore stand alone as the top of the provenance chain: `model`, `model_version`,
and the `params` snapshot carry everything evaluation and monitoring need, and nothing
downstream of `runs` changes when the jobs table arrives later as a new table.

### 7.5 Evaluation results

Evaluation outputs are stored relationally so accuracy is queryable history rather than
files. Two observations shape the design. First, an evaluation is itself a computation
with provenance, so it gets a run table with an as-used snapshot, like forecasting.
Second, an accuracy metric **is a time series**: of the coordinates that identify a value
— which forecasts (`run_name`), which target (`series_id`), which as-of slice
(`filtering`), which window (`win`), which quantile, which metric — every one is constant
along the time axis; only the window position and the value vary. The store's own doctrine
for that shape (§5.1) is identity in a small dimension table with a bigint surrogate,
points in a narrow fact table (the series-table-plus-samples design proven by
Prometheus-on-TimescaleDB). Shape to be validated during the spike; see §11.

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

**Why the dimension exists** (and why a flat labels-on-every-row table was rejected): the
label tuple repeats identically for every window position of every run of the same
monitor, and factoring it out roughly halves the fact heap and better than halves the
index — savings that *grow* with history, since the dimension is fixed while `ts`
positions accumulate forever. The dimension amortizes across re-runs too, because it is
keyed by `run_name` rather than `eval_run_id` — a nightly drift monitor appends points to
the same `eval_series_id` rows for years. That works because `filtering` and `win` are
**relative specifications** (`D-1T06:00` is a per-delivery cutoff rule, `7D` a window
size — never absolute moments), so the tuple is run-invariant by construction; what varies
between runs is which `ts` positions exist to score, and that lives on the facts. A config
change (new window, different gate closure) mints new series via get-or-create and the old
ones simply stop receiving points — an honest discontinuity, since scores under different
cutoffs aren't comparable anyway. At monitoring scale the fact table converts to a
hypertable with `segmentby eval_series_id, orderby ts` — the canonical compression shape.
Writers resolve ids get-or-create (same resolver pattern as `get_series_id`, §5.1), which
also buys write-time integrity: a fact row cannot reference a grid cell that was never
materialized as a dimension row.

**Labels are canonical strings, not structures.** The dimension's `filtering`, `win`, and
`quantile` columns are identity labels; their only duties are uniqueness, grouping, and
`WHERE`-clause legibility. The structured, machine-usable form of the evaluation config —
the thing that actually builds cutoff predicates — lives in `evaluation_runs.params`.
Labels use the types' standard serializations (`PT36H` is the ISO-8601 duration form,
`D-1T06:00` the canonical as-of spec), and the resolver **canonicalizes before
get-or-create** (parse → canonical format → lookup), so near-duplicate spellings
(`D-1T6:00`) cannot mint duplicate series — the same bijective-formatter rule that governs
quantile column names (§7.3). jsonb labels were considered and rejected: structural
equality adds degrees of freedom to the duplicate problem (key order, optional fields)
while making the dashboard predicate worse, and the structured form already exists in
`params`. Reads speak labels through a generated view joining the
dimension (§5.1 rule 3) — `WHERE metric = 'rMAE' AND win = '7D'` filters a tiny indexed
table, then range-scans narrow facts.

The run/series/points split also fixes three holes a single flat table would carry:

- **The reproducibility pin had nowhere to live.** §9.2's frozen backtest hinges on the
  `frozen_at` data pin — and in a flat metrics table that pin evaporates the moment the
  query finishes. `evaluation_runs.params` is the as-used snapshot for evaluations, exactly
  as `runs.params` is for forecasts. Re-execution needs nothing outside the run row:
  `run_name` says what was scored, `params` says how (filterings, windows, quantiles,
  providers, masks, period) and against which data (`frozen_at`, guarding every belief-log
  read) — and given the pin, every read is deterministic because the points tables are
  append-only with `recorded_at`.
- **Re-evaluation is a new belief, not an overwrite.** Scoring the same `ts` again after
  settlement revisions (revisioned actuals restate for weeks) is a new fact row under the same
  `(eval_series_id, ts)` with the new `eval_run_id` — accuracy history is itself a belief
  log, read by the same latest-belief `DISTINCT ON` as everything else, with `eval_run_id`
  as the belief version.
- **Round-tripping needs the config.** Reconstructing an evaluation report requires the
  filterings and windows that produced it; `load_evaluation_output` reads them from
  `evaluation_runs.params` rather than parsing directory names. Resume checks
  (`has_evaluation_output`) are an indexed lookup on the small runs table.

**Placement rule** (the generalization that keeps `target_time` off `forecast.runs`): what
is constant across the *computation* — config, actuals pin, `recorded_at` — lives on the
run; what is constant along the *series* — the label tuple — lives in the dimension; what
varies point to point — `ts`, the belief version, the value — lives in the facts. One
report ⇄ one run row; one label tuple ⇄ one dimension row, shared across runs.

Scheduled monitoring additionally materializes per-point errors into a `forecast_errors`
hypertable, rolled up by generated continuous aggregates, so dashboards read rollups
instead of re-running vintage joins.

---

## 8. Integrity and enforcement

The store depends on every point row referencing a registered series. The question is how
that invariant is enforced, and the answer is shaped by a failure-mode asymmetry and by
ingest mechanics.

**The failure being prevented.** A write attributed to the wrong series — or to no
registered series at all — splits history and empties joins, discovered weeks later at
read time. The identity design (§5.1) removes most of the surface before enforcement even
enters: writers obtain ids through the strict resolver, so an unknown or typo'd name
errors at insert time, and because ids are never reused, pre-resolved mappings in bulk
pipelines cannot go stale. What remains is the writer that invents or hardcodes a raw id —
and such an unregistered point is not merely unindexed; it is *uninterpretable* (no
resolution, no role).

**Why foreign keys are not the default.** Postgres FK checks run as per-row triggers
(an SPI lookup taking `FOR KEY SHARE` on the referenced row) and never batch — a real tax
on bulk ingest. At high write concurrency, shared locks on the same few `series` rows
drive MultiXact growth, a known operational sharp edge. `NO ACTION` couples series
lifecycle to chunk retention (a series row cannot be removed until every referencing chunk
has aged out). And retrofitting means validating against a full, compressed hypertable.
For a convention aimed at high-ingest time-series workloads, a default that degrades
ingest is the wrong default.

**Default: `enforcement = "monitor"`.** Two layers, both on by default:

1. The SDK resolves names to ids through a client-side registry cache — effectively free,
   and safe to cache because ids are never reused (§5.1) — **mandatory** on every write
   path. SDK writers (the majority) get write-time prevention at zero database cost.
   Auto-registration on first write is permitted **only from explicitly declared
   metadata** (a dataset object that carries its resolution); bare frames error rather
   than having a resolution inferred from index spacing, which would re-introduce the
   silent-defaults failure. Races resolve inside `register_series()` via `ON CONFLICT`.
2. An **orphan/grid sweep** ships with the TimescaleDB layer as a generated function,
   `data_quality_sweep(scan_window)`: it scans the recent write window for ids absent
   from the registry (the invented/hardcoded-id case), off-grid `target_time`
   (`time_bucket` against the declared `sample_interval`; month-plus intervals have no
   fixed stride and are skipped), and — where declared — `target_time_observed` outside
   its target's bucket (§6.1). Scheduling is deployment-owned (cron, `add_job`); alerts
   go through the standard hooks. Data quality is thereby the third alarm type beside model drift and
   asset anomaly, on the same monitoring machinery. The same sweep can flag anomalous
   `recorded_at − available_at` gaps — retro-stamped writes and unusually late arrivals —
   and the measured lag distribution is what freshness alerting calibrates from, rather
   than any declared value.

Non-SDK writers (Spark jobs, dbt models, agents writing SQL) get the same write-time
prevention in their own language: `get_series_id()` (strict) and `register_series()`
(get-or-create) are plain SQL functions, so the convention is never hostage to the Python
SDK; the sweep remains as the backstop for writers that bypass them with raw ids.

**Opt-in: `enforcement = "fk"`.** A deferrable FK (checks settle at commit, allowing
points-then-registration in one transaction) remains documented for deployments where it
is rational: low-volume forecast-only stores, regulated environments, dev/test.

**Opt-in: `append_only_guard`.** A generated `BEFORE UPDATE` trigger on revisioned
points tables that always raises — structural enforcement of §4.1's first law for
deployments that want it in schema rather than convention. It costs nothing on the write
path (no legitimate path ever UPDATEs a points table) and is verified inert under
compression, decompression, policy jobs, and DML over compressed chunks. Single-belief actuals
carry their `belief_guard` (§6.1) unconditionally, which subsumes it.

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
is "current best forecast" for serving; the generated `latest_<table>` view covers
the hot dashboard path. **The same query against `predictors` (on `available_at`) is
leakage-free feature assembly** — one query shape serves both serving and
point-in-time-correct input selection.

**Why `DISTINCT ON` rather than `last()`.** Over the mandated
`(series_id, target_time, available_at DESC)` index, the planner has two good plans for
this query and picks by cost. When revision depth is shallow (each target has a few
vintages), the winner is a plain ordered index scan of the contiguous
`(series, target-range)` slice with a Unique on top — sequential leaf reads, near-free.
When revision streams are deep (an hourly re-forecaster leaves dozens of vintages per
target), TimescaleDB's SkipScan node wins instead: one btree descent per (series, target)
group, skipping the superseded vintages, with gains proportional to revision depth — and
costs proportional to group count, so it is *not* automatically better; on a wide target
range with shallow revisions the per-bucket descents lose to the sequential scan. The
point for the convention is that `DISTINCT ON` leaves this choice to the planner —
same SQL, same index, both regimes covered (and on plain Postgres it is simply the
ordered scan). `last(col, available_at)` forecloses both: always a full aggregate scan
with `GROUP BY` machinery and one call per column — eight redundant aggregate states to
reconstruct a wide row `DISTINCT ON` returns whole. `last()` is instead the **required**
form where `DISTINCT ON` is unavailable: under any `GROUP BY`, as in context assembly
(§9.3) — and inside a continuous-aggregate definition, should a deployment materialize
one (§11 records why the generator deliberately emits none).

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
instant (system clock) — re-running with the same pin returns identical rows forever, no
matter what arrived since. The pin must guard **every belief-log read in the query** —
forecasts as well as actuals — because `available_at` is a writable claim on both:
a backtest writing simulated vintages with past `available_at` into the same `run_name`
would otherwise change an already-computed evaluation on re-run. The SDK captures one
`frozen_at` at evaluation start, uses it for all reads, and records it in
`evaluation_runs.params`. For live monitoring, set the pin to `now()` and the predicates
are no-ops.

**Pin placement is the semantics.** Four placements of the same predicate answer four
different questions. *Unpinned* (pin = `now()`): the store as it stands — the
`available_at` cutoffs still prevent leakage, but late backfills and settled revisions
are visible, so results legitimately improve as history does: the best current estimate
of model skill. *Frozen* (one pin, stamped at experiment start, recorded in
`evaluation_runs.params`): reproducible forever, per above. *Operational* (a rolling
pin — `recorded_at <= S` per simulated origin S, alongside the `available_at <= S`
cutoff): the `available_at` cutoff alone simulates what was *theoretically* knowable at
S, but operational reality lags theory — data knowable at S may not reach the system
until later — so the rolling pin scores the system *as it actually ingested*. The two
clocks separate "how good is the model" from "how good is the model as operated"; over
backfilled history the operational mode correctly degenerates (nothing was operationally
present before its load date — fail loud, §6.1). *Forensic* (one pin at one decision's
timestamp, a single read): reconstructs what the store held when the decision was made,
separating a wrong model from inputs that were late or have since been revised.

Per-quantile pinball-loss terms are enumerated per band column and **emitted by the
generator** from the declared band, e.g.
`avg(greatest(0.05*(ea.value-ef.q05), -0.95*(ea.value-ef.q05))) AS pinball_q05`; averaging
across the band approximates CRPS. Cross-quantile metrics (coverage, sharpness, crossing
checks) are plain row expressions — a structural benefit of the wide layout. Monitoring
variants use a `LEFT JOIN` with a configurable arrival-lag guard to distinguish "actual not yet
arrived" from "actual overdue for a realized period" (a data-quality alarm).

### 9.3 Context assembly

Model context windows must be one row per bucket, gaps explicit, ending at the last
complete bucket. The SDK issues `time_bucket_gapfill` over the relevant belief log, with
fill strategy per connector (`locf` for step-wise series, `interpolate` for smooth physical
signals, NaN passthrough only for models that handle it) and a configurable gap budget
(beyond N filled buckets: shorten the context or skip the run and alert). On revisioned
sources, a `DISTINCT ON … ORDER BY available_at DESC` CTE runs first so gapfill
regularizes the latest belief, not the revision stream — or, on TimescaleDB with buckets
at the series' native resolution (one `target_time` per bucket), `last(value,
available_at)` inside the gapfill aggregation (`locf(last(value, available_at))`)
expresses latest-belief inline and collapses the two passes into one. The declared bounds become
`context_start`/`context_end` on the run — the window declared is provably the window
queried — and gap statistics land in `runs.params`.

**Store-served covariates complete the leakage audit.** An engine-fed frame can only bound
the *target's* knowledge time (`context_end` derived from the last observed target value;
covariate publication times are simply absent from a plain frame — see the OpenSTEF
adapter's `context_end_method` provenance). When context assembly reads covariates from
`predictors` instead, the as-of cutoff makes the read leakage-free *by construction* — a
vintage published after the cutoff cannot be returned — and the SDK records that cutoff in
`runs.params` (`covariates_asof`), so the audit covers both halves of the input: measured
history via `context_end`, covariate vintages via the recorded cutoff.

---

## 10. Validation: the OpenSTEF spike

**The harness.** [OpenSTEF](https://github.com/OpenSTEF/openstef) (LF Energy) is the
leading open-source short-term energy forecasting pipeline, production-proven across
thousands of grid locations at the Dutch DSO Alliander, and its 4.x documentation is
explicit that storage is the user's responsibility ("you own I/O"; the 3.x database layer
has no 4.x equivalent). That makes it the ideal validation harness: an opinionated,
production-proven pipeline *we do not control*, with a storage-shaped hole — if the
convention backs it cleanly, the design generalizes beyond our own examples.

**What the review established.** OpenSTEF 4.x is bi-temporal *in memory* — its
`(timestamp, available_at)` versioning and knowledge-cutoff machinery map one-to-one onto
this convention's clocks — but it has **no bi-temporal layer at rest**, and production
`predict()` output carries **no knowledge time at all** (vintage stamping exists only
inside the backtest loop). Its public liander2024 benchmark exercises the convention
hard: a 7-quantile band, weather shipped as versioned vintage histories, versioned
measurements, and evaluation at a relative gate-closure cutoff (`D-1T06:00`) — the exact
shape of §9.2.

**What was built and validated (2026-08-25).** Adapters for every integration seam —
the production write path (`ForecastStoreCallback`), store-served context assembly
(`StoreReader`/`ForecastFeed`), and the benchmark harness
(`TimescaleTargetProvider`/`TimescaleBenchmarkStorage`) — implemented and live-tested
against a TimescaleDB store, every store provisioned through the generator per the
dogfooding rule. Phase A verified real knowledge time (claim ≈ measurement; leakage audit
passes; vintage isolation: a vintage published after the decision moment is invisible at
it). Phase B ran openstef-beam's real `BenchmarkPipeline` with the store as its only
source and sink, validated against an in-memory control: quantile points round-trip
exactly, evaluation metrics round-trip losslessly, subset frames re-derive from stored
artifacts and equal the originals, resume checks short-circuit as indexed queries, and
simulated knowledge time landed in the same column production writes real time
(`recorded_at > available_at` across all backtest runs — §4.3 in running code). The
deferred seam (the `VersionedTimeSeriesDataset` pushdown repository) remains the
upstream-contribution candidate.

**The flag-plant run:** one liander2024 wind park ingested — 35,133 measurements carrying
the dataset's **real per-row publication claims** and 591k weather vintage rows — then
Amazon's Chronos-2 (BASE, ONNX) ran OpenSTEF's own benchmark wiring for 7 benchmark days:
28 simulated vintages, 8,092 forecast points, **rCRPS 0.0906, rMAE@q50 0.1248**,
calibration near-nominal across the band. The summary is a SQL query against
`evaluation_series`/`evaluation_metrics` — §2's "accuracy as a queryable time series,"
literally.

**Findings** (each publishable independent of the integration):

- The flagship open forecasting pipeline is **bi-temporal in RAM with no bi-temporal
  layer at rest**; production forecasts leave it with **no knowledge time recorded** —
  the store's write path is that recorder (demonstrated, not argued).
- Its backtest requires **writable knowledge time** — SQL:2011 system time cannot serve
  the workload; the same column now demonstrably holds real (production) and simulated
  (backtest) claims, distinguished by `recorded_at`.
- **Found validation for revisioned actuals:** the liander measurement feed itself publishes with
  ~48-hour per-row settlement lags — revisioned, claim-bearing actuals are how real grid
  data already arrives.
- Its evaluation config independently uses the **relative gate-closure cutoff** this
  convention canonicalizes.

The adapter-level reference — attach points, type and configuration mappings, adapter
lessons, packaging notes, and reproduction commands — lives in
[`integrations/openstef.md`](integrations/openstef.md).

---

## 11. Open questions

| Status | Item |
|---|---|
| closed | Auxiliary per-point statistics from engines (e.g. OpenSTEF's optional `stdev`) — **not persisted.** The spike gathered the deciding evidence: `stdev` flows through backtest outputs and evaluation subsets but nothing downstream of the forecaster consumes it, so it is auxiliary, not a value column. Adapters record its presence in `runs.params` (`stdev_column_present`); reopen only if a consumer appears. |
| closed | `evaluation_runs`/`evaluation_series`/`evaluation_metrics` shape — **validated.** The run's `params` snapshot captures enough to reconstruct derived evaluation subsets exactly (metrics round-trip losslessly; subsets re-derive from stored backtest output + ground truth), and the get-or-create dimension resolver held up under a real harness. Evidence: the OpenSTEF `EvaluationReport` round-trip ([integrations/openstef.md](integrations/openstef.md)). |
| open | Multi-producer / composed forecasts — ensembles and hierarchical compositions where several producers forecast the same target (the general form of OpenSTEF's `{learner}__{quantile}` ensemble columns, which smuggle a producer dimension into column names). Candidate: one run per producer + one for the composition, with member `run_id`s recorded in the composition run's `params` — the run is already the store's producer dimension. Unexercised by the spike (single-model runs); decide when the first composed workflow is ported. |
| open | Period-valued targets (delivery products as `tstzrange`) — unexercised by OpenSTEF's point grid; low priority. |
| closed | Actuals unification — **resolved: columns unified; revisions are the PK switch** (2026-08-26). The old "tier" bundling (tier 1 = no claim column) conflated claims with revisions and made §4.2's own day-ahead-price example unstorable without revision machinery. Now both shapes carry the three clocks; `available_at` is the universal knowledge clock (the §9.2 special case is gone); single-belief writes are skip-or-raise via the generated `belief_guard` trigger (`ConflictingBelief`, never a silent swallow — live-verified inert under all TimescaleDB internals); revisioned idempotency is retcon-refusal by doctrine. Tier numbers retired the same day: declarations carry `revisions: bool` (`ActualsSpec(revisions=)`, `StoreConfig(actuals_revisions=)`), prose says single-belief / revisioned. Convention 0.4.0. |
| open | Per-series revision-expectation contract — inside a shared revisioned table the sweep cannot tell an expected revision (settlement) from an incident (a publish-once feed restating); routing that alarm needs a per-series declaration (sketch: `series.metadata->'contract'->>'single_belief'`). Decide when the first publish-once external feed is ingested. |
| open | TimescaleDB-required posture — direction set (2026-08-26): the convention will require TimescaleDB rather than treat it as an optional acceleration layer (the §8 sweep already standardizes on `time_bucket` and ships with the TimescaleDB layer; plain-Postgres stores get SDK write-path validation only). When this lands, sweep §1/§4.3's any-Postgres-14+ language. |
| open | Continuous aggregates — **none generated yet; two sanctioned homes.** A cagg hard-codes the two questions every belief-log read must answer (`asof = now()`, no pin), so caggs serve only current-knowledge surfaces — of which two are real. (1) **Model-input downsampling**: models consume a fixed number of *timesteps* (TSFM checkpoints freeze `context_length`; history beyond it is truncated), so input resolution determines how much calendar the model sees — production forecasts read derived, coarser series (hourly load from 10-second telemetry), and over **single-belief** telemetry — prompt, in-order, revision-free, never written by backtests — a cagg is the right maintainer of the derived series' current-knowledge view, with the run recording the cagg watermark as read provenance. (Over a revisioned source, vintage-select *below* the aggregate — a hierarchical cagg whose first level is `last(value, available_at)` per point — or plain `avg` double-counts revised points; better, declare a revision-free source single-belief and let the PK make the one-level cagg exact.) A derived series is a first-class registry series declaring its source and aggregate — and it is not a third pattern: **derivation preserves epistemic status**, inheriting the weakest among its inputs (all inputs realized → `actuals`, a recomputation is a correction; any input pre-realization → `predictors`, a newer row re-derives from a newer vintage). Original observations are never discarded at ingest — a derivation is re-derivable only while its inputs survive — and their home depends on what alignment loses. **Snapping loses a timestamp**: the optional nullable `target_time_observed` column, specified in §6.1 — generator support (an `ActualsSpec` flag) arrives with the first jittered feed. **Aggregating loses a set**: N:1 derivations keep their inputs in an upstream raw table (often a pre-existing telemetry hypertable; §6.1 existing-table mode attaches it, `derived.source` makes the chain navigable) — raw streams as rows cannot satisfy the §4.1 grid contract. The raw table wants the trust rule too — `observed_at` the device's claim, `recorded_at` the witnessed arrival — and takes aggressive retention while the aligned series is kept. Backtests over the same inputs need bi-temporal aggregates instead: aggregate inside the as-of read, or materialize the derived series as a revisioned belief log via a job (recomputation after late data = a revision; the aggregate of a belief log is itself a belief log). (2) **Evaluation rollups** (§7.5): a job materializes per-point residuals into `forecast_errors` — asof and gate answered once, at write time — and caggs roll those up for dashboards. What stays out: caggs directly over revision-bearing or forecast tables (aggregation across vintages is meaningless before vintage selection; late writes churn materialized ranges) and any pinned surface. Later, alongside whether materialized-only context reads are mandated or merely recorded. |
| open | Connector band-mapping statistics (requested vs. interpolated quantiles) — one spec paragraph + `params` provenance flag. The OpenSTEF path sidesteps it (workflow configs request the band directly, and the adapter rejects off-band quantiles — verified); needed when the first interpolating connector (Chronos native quantiles outside a store's band) lands. |
| open | `forecast.jobs` control-plane table — deliberately deferred past the MVP (§7.4): its only consumers (worker, data-arrival triggers, agent-managed pipelines) are later-stage, and job definitions are caller-owned until then. Arrives as a new table plus a nullable `runs.job_id` column; nothing downstream of `runs` changes. |
| closed | Multi-instance roles — **shipped.** `StoreConfig.extra_tables` declares additional instances (`ForecastLogSpec` with its own band, `PredictorLogSpec`, `ActualsSpec` with its own PK shape); the generator emits each extra table identically to a canonical one (DDL, as-of index, serving view, `store_tables` declaration, columnstore) from one instance-plan source. Reads/writes take table names validated against the instance's own declaration; provisioning is additive (stored tables absent from a config are untouched; drift is checked per declared table). The workspace driver is realized: `TimescaleBenchmarkStorage(forecast_table=...)` keeps experiment artifacts out of production history. Live-verified additively on the provisioned store. |
| closed | Chunk skipping — **removed** (2026-08-25). The generator briefly enabled knowledge-clock chunk skipping (early access, ≥ 2.16); dropped entirely after issues surfaced with the feature. Knowledge-clock predicates rely on `target_time` partition pruning plus orderby minmax sparse indexes; revisit if the feature matures to GA. |
| open | Per-series expected arrival lag — deliberately cut from the MVP as a declared value. Arrival is *measured* everywhere (`recorded_at`), so if freshness alerting later needs an expectation, calibrate it from observed gaps (`recorded_at − available_at`) rather than declaring it. Revisit when monitoring ships. |

Decisions already closed in this draft: quantile representation (wide, generative,
band-in-metadata — §7.3); point/mean forecasts (`mean` column — §7.3); versioned inputs as
a second belief-log instance (§6.2); uniform `available_at` vocabulary (§4.1); the metadata
layer (§5); enforcement defaults (monitor-first, FK opt-in — §8); namespacing (dedicated
`forecast` schema — §5); configuration split (registry / caller config / runs, with the
in-store jobs table deferred past the MVP — §7.4); series
identity (bigint surrogate + strict SQL resolver, names in the registry — §5.1); no
`kind` column — a series is a quantity, tables are belief types about it, and read
APIs take table names resolved against `store_tables` (§5.2); predictor
point values as `value`, with the statistic in registry metadata (§6.2); the
forecasts-vs-predictors decision test as provenance, not origin (§6.2).

---

*Design review conducted against the openstef monorepo @ v4.3.1 (`openstef-core`
datasets/types, `openstef-beam` benchmarking/evaluation, `openstef-models`
presets/callbacks, the liander2024 benchmark, and the deployment examples).
Implementation validated live 2026-08-25: 63-test suite against a Tiger Cloud TimescaleDB
instance plus the liander2024 Chronos-2 benchmark run (§10), openstef packages installed
from PyPI at 4.x.*
