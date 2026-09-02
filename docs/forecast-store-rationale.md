# Forecast Store Convention — Design Rationale

Companion to [`forecast-store-convention.md`](forecast-store-convention.md): the reasoning,
alternatives considered, worked examples, and full validation narrative behind each
numbered rule in the spec. Section numbers mirror the spec — read the spec first; come
here for the "why."

---

## §4.1 — Naming and clock rationale

**Why `available_at`, not `created_at`.** Hand-rolled schemas call this column
`created_at`, which sounds like system time — a machine-stamped physical fact — while the
column must accept written pasts (backfills, backtests). In live operation, compute time,
availability, and ingest all coincide, so the misnomer hides. The first backtest then
forces a value that's false under one reading of "created," or breaks vintages under the
other.

**Why `recorded_at` exists at all.** Because `available_at` is a writable claim, it can't
by itself make backtests trustworthy: a row inserted at 09:00 but stamped `available_at =
06:00` would let a backtest "see" data production never had, and an honest late arrival
would silently change a past backtest's result for the same cutoff. `recorded_at` — a
system-stamped, never-written measurement — resolves both: it pins reproducible backtests
(§9.2) and makes every retro-stamped write detectable rather than silent.

## §4.2 — The day-ahead price worked example

Publication *timing* plays no part in the actuals/predictors test; what matters is where
realization falls, and it need not fall at `target_time`. The day-ahead price for
Wednesday 18:00 comes into existence on *Tuesday*: order books close at 12:00 CET, the
clearing algorithm crosses aggregate supply and demand, and one binding price per delivery
period publishes around 12:45. Before that run there is no price, only forecasts of it;
after it, there is a fact that only an exchange error could restate. The auctioneer does
not predict the price — the clearing computation creates it ("day-ahead" names the
product, not a guess about the day ahead). Exchange publications are therefore `actuals`,
with `available_at` a day before `target_time`, while a desk's Monday forecast of the same
price is written before the value exists and belongs in `predictors` — scoreable against
the cleared price once it exists.

## §4.3 — Why not SQL:2011 temporal features, in full

Postgres 18/19's temporal machinery models **versioned state**: one true row per key,
revised in place, system time as an audit trail the system stamps. It also spans only two
of the spec's three clocks — valid and transaction time; the knowledge clock has no
standardized counterpart. The forecast store is the opposite: an immutable belief *log*
where many vintages per target coexist as first-class data, and where knowledge time must
be **writable** — bulk loads and simulated backtests set it explicitly. `WITHOUT OVERLAPS`
on a (series, target) key would forbid vintages outright, and `AS OF` syntax cannot
express the workhorse query (argmax-over-knowledge-time with relative cutoffs).

## §5.1 — Series identity: the natural-text-key alternative

*Alternative considered — natural text key* (`series_id text` in every table): join-free
queries and self-describing rows in psql, but permanently larger, never-compressed index
entries and collated sorts on the store's biggest tables. The strict resolver reverses the
integrity comparison anyway: a typo'd name errors at write time under the bigint design,
where the text design would silently create a phantom series for a later sweep to find.

**Why tags beat a typed grouping column.** Real fleets segment along several axes at once
(region × asset type × customer), which one flat column can't express and arbitrary tag
keys can — grouped rollups still work fine on a jsonb extraction
(`metadata->'tags'->>'group'`).

## §5.2 — Why role is declared, not inferred

Role is not inferred from a table's shape — declaration beats introspection — because
inference would let two identically-shaped tables mean different things silently. A
misrouted role is a misrouted page, not a mislabel: monitoring dispatch (missing-data
alarms watch actuals; publication-lag watches predictors; drift watches forecast logs)
depends on it being right. The generator deriving mechanical keys *from* the role is what
keeps declaration and DDL from drifting apart over time.

The §4.2 classification rule is really a rule about **which table a writer sends a stream
to**, not a registration step — which is what makes additional instances (a second
forecast table) readable with zero new machinery: just another row in `store_tables`.

## §6.1 — Actuals: backfill and jitter details

A claimless backfill defaults `available_at` to its load date, and a knowledge-cutoff
backtest run over it fails loud rather than quietly optimistic — the lazy path stays the
safe path.

**Why `target_time_observed` is nullable.** A NOT NULL contract would push mixed-quality
feeds toward fabricating `target_time` as the observation — jitter-zero lies that poison
the diagnostic. NULL honestly means "not recorded."

## §6.2 — Why predictors use `value`, not `mean`

This is the `mean`≠`q50` honesty rule in reverse: a vendor's point value is often a
deterministic run or a median rather than a true mean, so a `mean` column on `predictors`
would assert a statistic the feed may not actually provide. A *known* statistic is
recorded as per-feed registry metadata (`metadata->>'statistic'`) instead.

## §7.2 — Forecast points: columnstore and hypertable-conversion rationale

**Columnstore settings.** `segmentby = 'series_id'` because it's the primary filter with
high row density per chunk; `orderby` on the two clocks because adjacent rows in a segment
are then a target's consecutive vintages — a natural progression that compresses well —
and orderby columns get minmax sparse indexes automatically, which are exactly the as-of
predicates; a 7-day columnstore policy because beliefs are immutable on write, but the
recent window stays rowstore for the hot serving path.

**Why convert after creation, not `CREATE TABLE … WITH (tsdb.*)`.** The `WITH` form can't
retrofit an existing plain-Postgres table (`IF NOT EXISTS` no-ops on it), so it would
leave plain-Postgres deployments no upgrade path when the extension later appears.
Converting after creation keeps one canonical DDL that's identical on every Postgres.

**One instance vs. more.** The legitimate drivers for a second forecast-log instance are
all per-hypertable policies that genuinely differ: retention split between production
history and an experiment workspace, chunk intervals for disjoint cadence domains, tenancy
(erasure as `DROP TABLE`). The cost of splitting is what a single table buys — cross-model
evaluation as one query, one serving view, one as-of surface — so the default stays one,
and splitting requires a policy that actually differs. The workspace driver (retention
split) is validated in the OpenSTEF benchmark harness (§10).

## §7.3 — Quantile representation: alternatives considered

**Long/narrow** (one row per quantile, `quantile` as a numeric column). Gives one
permanently stable DDL and quantile-generic per-quantile rollups, but each value pays the
full key tuple (~70–90 bytes of key and row header per 8-byte value, and roughly
band-size-times the rows and index entries — indexes never compress); the hot serving path
needs a pivot on every read; a logical forecast becomes multiple rows, admitting torn
partial writes; and the genericity argument fails exactly where it matters most —
cross-quantile metrics (interval coverage, sharpness, quantile-crossing checks) need
multiple levels of the same point in one expression, which wide rows give for free.

**jsonb quantiles** (`{"0.05": 11.2, …}` per row). Flexibility of long with the row count
of wide, but several-fold worse compression on the store's largest table (keys repeated in
every row, values as opaque blobs, no per-column chunk exclusion); an extraction and cast
in every canonical query; a text-key normalization footgun (`'0.5'` vs `'0.50'` vs `'q50'`)
that moves errors from write time to silent read-time NULLs; hostile to BI and
per-quantile aggregation. The principled line: jsonb for run-scoped metadata that's
occasionally filtered (`runs.params`), typed columns for point-scoped measures that
everything aggregates.

**One frozen wide superset** (bless N columns forever). Any fixed set eventually fails —
`q2.5/q97.5`, the standard 95% interval, is already awkward — and every miss would be a
spec break in a document meant to be cited as stable. The generative convention (§4.4)
keeps wide's physics without freezing the set.

## §7.4 — The deferred `forecast.jobs` table

The middle row of §7.4's table has an obvious database home: a `forecast.jobs` table
(engine, series bindings, horizon, quantiles, trigger spec, engine config) that would make
the store a control plane — a standalone worker polls it, an orchestrator task becomes
`run_job(job_id)`, an agent creates a pipeline by inserting a row, and job registration
validates quantiles ⊆ band and series bindings before any run executes. It's deliberately
not part of the MVP: its only consumers (a worker, data-arrival triggers, agent-managed
pipelines) are later-stage. Runs stand alone as the top of the provenance chain in the
meantime — `model`, `model_version`, and the `params` snapshot carry everything evaluation
and monitoring need — and nothing downstream of `runs` changes when the jobs table
arrives.

## §7.5 — Why evaluation results use a dimension table

A flat labels-on-every-row table was rejected: the label tuple repeats identically for
every window position of every run of the same monitor, and factoring it out roughly
halves the fact heap and better than halves the index — savings that grow with history,
since the dimension is fixed while `ts` positions accumulate forever. The dimension
amortizes across re-runs too, because it's keyed by `run_name` rather than `eval_run_id` —
a nightly drift monitor appends points to the same `eval_series_id` rows for years, since
`filtering` and `win` are relative specifications and so the tuple is run-invariant by
construction.

**Labels are canonical strings, not structures.** The dimension's `filtering`, `win`, and
`quantile` columns exist only for uniqueness, grouping, and `WHERE`-clause legibility. The
structured, machine-usable form of the evaluation config — the thing that actually builds
cutoff predicates — lives in `evaluation_runs.params`. Labels use each type's standard
serialization (`PT36H` is the ISO-8601 duration form, `D-1T06:00` the canonical as-of
spec), and the resolver canonicalizes before get-or-create (parse → canonical format →
lookup), so near-duplicate spellings can't mint duplicate series. jsonb labels were
considered and rejected: structural equality adds degrees of freedom to the duplicate
problem (key order, optional fields) while making the dashboard predicate worse, and the
structured form already exists in `params`.

**Three holes a flat table would have left open:**

- *The reproducibility pin had nowhere to live.* §9.2's frozen backtest hinges on the
  `frozen_at` data pin, which would evaporate the moment a flat query finished.
  `evaluation_runs.params` is the as-used snapshot for evaluations, exactly as
  `runs.params` is for forecasts.
- *Re-evaluation is a new belief, not an overwrite.* Scoring the same `ts` again after
  settlement revisions is a new fact row under the same `(eval_series_id, ts)` with a new
  `eval_run_id` — accuracy history is itself a belief log.
- *Round-tripping needs the config.* Reconstructing an evaluation report requires the
  filterings and windows that produced it; reading them from `evaluation_runs.params`
  avoids parsing directory names.

## §8 — Why foreign keys are not the default

Postgres FK checks run as per-row triggers (an SPI lookup taking `FOR KEY SHARE` on the
referenced row) and never batch — a real tax on bulk ingest. At high write concurrency,
shared locks on the same few `series` rows drive MultiXact growth, a known operational
sharp edge. `NO ACTION` couples series lifecycle to chunk retention (a series row can't be
removed until every referencing chunk has aged out). And retrofitting means validating
against a full, compressed hypertable. For a convention aimed at high-ingest time-series
workloads, a default that degrades ingest is the wrong default.

## §9.1 — `DISTINCT ON` vs. `last()`, in full

Over the mandated `(series_id, target_time, available_at DESC)` index, the planner has two
good plans for the as-of query and picks by cost. When revision depth is shallow (each
target has a few vintages), the winner is a plain ordered index scan of the contiguous
`(series, target-range)` slice with a `Unique` on top — sequential leaf reads, near-free.
When revision streams are deep (an hourly re-forecaster leaves dozens of vintages per
target), TimescaleDB's SkipScan node wins instead: one btree descent per (series, target)
group, skipping superseded vintages, with gains proportional to revision depth — and costs
proportional to group count, so it's not automatically better; on a wide target range with
shallow revisions, per-bucket descents lose to the sequential scan. `DISTINCT ON` leaves
this choice to the planner: same SQL, same index, both regimes covered (and on plain
Postgres it's simply the ordered scan). `last(col, available_at)` forecloses both — it's
always a full aggregate scan with `GROUP BY` machinery and one call per column, i.e. eight
redundant aggregate states to reconstruct a wide row that `DISTINCT ON` returns whole.
`last()` is the *required* form only where `DISTINCT ON` is unavailable: under any `GROUP
BY`, as in context assembly (§9.3), and inside a continuous-aggregate definition, should a
deployment materialize one (§11 records why the generator emits none by default).

## §10 — The OpenSTEF spike, full narrative

**The harness.** [OpenSTEF](https://github.com/OpenSTEF/openstef) (LF Energy) is the
leading open-source short-term energy forecasting pipeline, production-proven across
thousands of grid locations at the Dutch DSO Alliander, and its 4.x documentation is
explicit that storage is the user's responsibility ("you own I/O"; the 3.x database layer
has no 4.x equivalent). That makes it the ideal validation harness: an opinionated,
production-proven pipeline we do not control, with a storage-shaped hole — if the
convention backs it cleanly, the design generalizes beyond our own examples.

**What the review established.** OpenSTEF 4.x is bi-temporal *in memory* — its
`(timestamp, available_at)` versioning and knowledge-cutoff machinery map one-to-one onto
this convention's clocks — but it has no bi-temporal layer at rest, and production
`predict()` output carries no knowledge time at all (vintage stamping exists only inside
the backtest loop). Its public liander2024 benchmark exercises the convention hard: a
7-quantile band, weather shipped as versioned vintage histories, versioned measurements,
and evaluation at a relative gate-closure cutoff (`D-1T06:00`) — the exact shape of §9.2.

**What was built and validated (2026-08-25).** Adapters for every integration seam — the
production write path (`ForecastStoreCallback`), store-served context assembly
(`StoreReader`/`ForecastFeed`), and the benchmark harness
(`TimescaleTargetProvider`/`TimescaleBenchmarkStorage`) — implemented and live-tested
against a TimescaleDB store, every store provisioned through the generator per the
dogfooding rule. Phase A verified real knowledge time (claim ≈ measurement; leakage audit
passes; vintage isolation: a vintage published after the decision moment is invisible at
it). Phase B ran openstef-beam's real `BenchmarkPipeline` with the store as its only source
and sink, validated against an in-memory control: quantile points round-trip exactly,
evaluation metrics round-trip losslessly, subset frames re-derive from stored artifacts and
equal the originals, resume checks short-circuit as indexed queries, and simulated
knowledge time landed in the same column production writes real time (`recorded_at >
available_at` across all backtest runs — §4.3 in running code). The deferred seam (the
`VersionedTimeSeriesDataset` pushdown repository) remains the upstream-contribution
candidate.

**The flag-plant run.** One liander2024 wind park ingested — 35,133 measurements carrying
the dataset's real per-row publication claims and 591k weather vintage rows — then
Amazon's Chronos-2 (BASE, ONNX) ran OpenSTEF's own benchmark wiring for 7 benchmark days:
28 simulated vintages, 8,092 forecast points, rCRPS 0.0906, rMAE@q50 0.1248, calibration
near-nominal across the band. The summary is a SQL query against
`evaluation_series`/`evaluation_metrics` — §2's "accuracy as a queryable time series,"
literally.

**Findings** (each publishable independent of the integration):

- The flagship open forecasting pipeline is **bi-temporal in RAM with no bi-temporal layer
  at rest**; production forecasts leave it with **no knowledge time recorded** — the
  store's write path is that recorder (demonstrated, not argued).
- Its backtest requires **writable knowledge time** — SQL:2011 system time cannot serve
  the workload; the same column now demonstrably holds real (production) and simulated
  (backtest) claims, distinguished by `recorded_at`.
- **Found validation for revisioned actuals:** the liander measurement feed itself
  publishes with ~48-hour per-row settlement lags — revisioned, claim-bearing actuals are
  how real grid data already arrives.
- Its evaluation config independently uses the **relative gate-closure cutoff** this
  convention canonicalizes.

The adapter-level reference — attach points, type and configuration mappings, adapter
lessons, packaging notes, and reproduction commands — lives in
[`integrations/openstef.md`](integrations/openstef.md).

*Design review conducted against the openstef monorepo @ v4.3.1 (`openstef-core`
datasets/types, `openstef-beam` benchmarking/evaluation, `openstef-models`
presets/callbacks, the liander2024 benchmark, and the deployment examples). Implementation
validated live 2026-08-25: 63-test suite against a Tiger Cloud TimescaleDB instance plus
the liander2024 Chronos-2 benchmark run, openstef packages installed from PyPI at 4.x.*

## §11 — Open questions, full history

**Actuals unification (closed 2026-08-26).** The old "tier" bundling (tier 1 = no claim
column) conflated claims with revisions and made §4.2's own day-ahead-price example
unstorable without revision machinery. Both shapes now carry the three clocks;
`available_at` is the universal knowledge clock (the old §9.2 special case is gone);
single-belief writes are skip-or-raise via the generated `belief_guard` trigger
(`ConflictingBelief`, never a silent swallow — live-verified inert under all TimescaleDB
internals); revisioned idempotency is retcon-refusal by doctrine. Tier numbers retired the
same day: declarations carry `revisions: bool` (`ActualsSpec(revisions=)`,
`StoreConfig.standard(actuals_revisions=)`), prose says single-belief / revisioned. Convention
0.4.0.

**Continuous aggregates (open) — the fullest open item.** None are generated yet; there
are two sanctioned future homes. (1) **Model-input downsampling**: models consume a fixed
number of timesteps (TSFM checkpoints freeze `context_length`), so input resolution
determines how much calendar the model sees — production forecasts read derived, coarser
series (hourly load from 10-second telemetry), and over **single-belief** telemetry
(prompt, in-order, revision-free, never written by backtests) a cagg is the right
maintainer of the derived series' current-knowledge view, with the run recording the cagg
watermark as read provenance. Over a revisioned source, vintage-selecting *below* the
aggregate — a hierarchical cagg whose first level is `last(value, available_at)` per point
— or plain `avg` would double-count revised points; better to declare a revision-free
source single-belief and let the PK make the one-level cagg exact. A derived series is a
first-class registry series declaring its source and aggregate, and it is not a third
pattern: **derivation preserves epistemic status**, inheriting the weakest among its
inputs (all inputs realized → `actuals`; any input pre-realization → `predictors`).
Original observations are never discarded at ingest — a derivation is re-derivable only
while its inputs survive. Snapping loses a timestamp (the optional nullable
`target_time_observed` column, §6.1); aggregating loses a set (N:1 derivations keep their
inputs in an upstream raw table, since raw streams as rows can't satisfy the §4.1 grid
contract) — the raw table wants the trust rule too (`observed_at` the device's claim,
`recorded_at` the witnessed arrival) and takes aggressive retention while the aligned
series is kept. Backtests over the same inputs need knowledge-aware aggregates instead:
aggregate inside the as-of read, or materialize the derived series as a revisioned belief
log via a job. (2) **Evaluation rollups** (§7.5): a job materializes per-point residuals
into `forecast_errors`, and caggs roll those up for dashboards. What stays out: caggs
directly over revision-bearing or forecast tables (aggregation across vintages is
meaningless before vintage selection; late writes churn materialized ranges), and any
pinned surface.

**Multi-instance roles (closed) — shipped.** `StoreConfig.with_tables(...)` declares
additional instances (`ForecastLogSpec` with its own band, `PredictorLogSpec`,
`ActualsSpec` with its own PK shape); the generator emits each extra table identically to
a canonical one (DDL, as-of index, serving view, `store_tables` declaration, columnstore)
from one instance-plan source. Reads/writes take table names validated against the
instance's own declaration; provisioning is additive. The workspace driver is realized:
`TimescaleBenchmarkStorage(forecast_table=...)` keeps experiment artifacts out of
production history. Live-verified additively on the provisioned store.

**Chunk skipping (closed) — removed 2026-08-25.** The generator briefly enabled
knowledge-clock chunk skipping (early access, ≥ 2.16); dropped entirely after issues
surfaced with the feature. Knowledge-clock predicates rely on `target_time` partition
pruning plus orderby minmax sparse indexes; revisit if the feature matures to GA.

**Remaining open items**, unchanged from the spec's summary table: multi-producer/composed
forecasts, period-valued targets, per-series revision-expectation contracts, the
TimescaleDB-required posture, connector band-mapping statistics, the deferred
`forecast.jobs` table (§7.4), and per-series expected arrival lag.
