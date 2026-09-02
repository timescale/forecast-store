# DX TODO — Python interface improvements

Developer-experience review of the `forecast_store` Python surface (2026-09-02).
Design stance to preserve while working these: SQL-first, thin SDK, caller
owns the transaction. Items are ranked by impact; check them off as they land.

## First slice (small, touches every user)

- [x] **1. Series registration has no Python entry point.**
  Every caller (README quickstart, tests, `scripts/ingest_liander.py`) ran raw
  SQL against `forecast.register_series(...)` with the schema hardcoded, so
  `config.schema` was ignored on the most common path.
  - [x] Add `register_series(conn, config, name, sample_interval, *, timezone=None, unit=None, description=None, metadata=None) -> int` (new `forecast_store/series.py`)
  - [x] Add `get_series_id(conn, config, name) -> int` raising `UnknownSeries` (`UnknownSeries` now lives in `series.py`, re-exported from `read.py`)
  - [x] Update README quickstart, tests, the ingest script, and both OpenSTEF integrations to use them

- [x] **2. Series identity flips between id and name.**
  Writes took `series_id`; reads took `series_name`. Callers had to hold both.
  - [x] One parameter, `series: SeriesRef` (`str | int`), on all three writes and both reads; resolved internally by one registry lookup that also supplies the grid for validation
  - [x] Ids stay accepted (pass an `int`); the keyword `series_id=` on `write_forecast_run` was renamed to `series=` (pre-release, no alias kept)

- [x] **4. Config must be redeclared by every client, though the store persists it.**
  `config.py` docstring promised any client could reconstruct the store's shape
  from `store_tables`, but there was no loader; the integrations defaulted to
  `StoreConfig()`, wrong for any non-default store.
  - [x] `StoreConfig.from_store(conn, schema="forecast")` — inverts `store_tables` via the new pure `ddl.config_from_tables` (round-trip tested); `append_only_guard` recovered from the catalog; raises `provision.NotProvisioned` when no store exists
  - [x] `StoreConfig` now canonicalizes `extra_tables` to name order so a loaded declaration compares equal to the declared one
  - [x] `ForecastStoreCallback`, `StoreReader`, `TimescaleTargetProvider`, `TimescaleBenchmarkStorage` load from the store on first use when `store_config` is omitted (`schema=` / `store_schema=` say where; a contradicting pair is refused at construction)
  - [x] `TimescaleTargetProvider` takes `store_config`; `actuals_revisions` removed, `store_schema` now optional; the storage adapter shares the provider's declaration

- [x] **6. Root `__all__` exports the wrong subset.**
  Write, read, provision, and the exceptions needed deep imports; the README
  itself imported from submodules.
  - [x] Root now re-exports the declaration types, `provision`/`ProvisionReport`, the series registry, all writes and reads, `forecast_asof`, naming, and every exception; README quickstart imports from the root. Integrations stay submodule-only (engine imports)

## Needs a short design note first

- [x] **3. Three write functions, three `points` shapes.**
  Forecasts: `(ts, {col: value})`. Actuals: `(ts, value)` or
  `(ts, value, observed)` depending on the table's stored declaration.
  Predictors: `(ts, available_at, value)`. The arity-by-declaration rule only
  fails at runtime.

  **Evidence gathered (2026-09-02):**
  - `write_actuals` cannot take per-row `available_at`; two callers drop to raw
    SQL for exactly that (`tests/test_benchmark_pipeline.py` seed INSERT,
    `scripts/ingest_liander.py` COPY).
  - `write_predictors` has no `table=` and hardcodes the `value` column, so an
    extra predictor instance (`PredictorLogSpec`, incl. a probabilistic band) is
    unwritable through the SDK.
  - Predictors require `available_at` per point even when one vendor run shares
    a single publication time.
  - DDL: forecasts `available_at NOT NULL` (from the run); predictors
    `NOT NULL`, no default; actuals `NOT NULL DEFAULT now()`.

  **Done (recommendations accepted, 2026-09-02):** generalized the forecast
  shape to all three writes; rules below are as implemented (`write.py`,
  tests in `tests/test_write_shapes.py`). A point is `(target_time, {column: value})`; `available_at`
  is a column like any other, with a call-level default; the table's stored
  declaration decides whether it is required, defaulted, or fixed.
  1. `Point = tuple[datetime, Mapping[str, object] | float | None]`. A bare
     scalar is sugar for `{"value": x}`, accepted only when the table declares
     exactly one value column.
  2. Writable keys per table = declared `value_columns` + the knowledge column
     (`available_at`) + `target_time_observed` where declared. Unknown keys
     fail before any INSERT, naming the table and its writable columns.
  3. `available_at` resolution by declaration: forecast logs — from the run's
     kwarg, a per-point key is rejected (a run has one knowledge time);
     actuals — per-point key, else call-level kwarg, else the column default
     (arrival measured); predictors — per-point key, else call-level kwarg,
     else error (publication must be stated).
  4. `write_predictors` gains `table="predictors"` and validates the declared
     role, as `write_actuals` already does. The two become mirror images:
     `(conn, config, series, points, *, available_at=None, table=...)`.
  5. `points` accepts any iterable (materialized once), so
     `df.to_dict("index").items()` works directly.
  6. Migration (pre-release, breaking): predictor 3-tuples ->
     `(ts, {"available_at": a, "value": v})` or a batch `available_at=`;
     actuals observed 3-tuples -> `(ts, {"value": v, "target_time_observed": o})`.
     11 call sites in tests/scripts/README; the benchmark seed moves onto the
     SDK; the ingest COPY stays (throughput).
  - Alternatives considered: flat records `{"target_time": .., ..}` (most
    SQL-like, pandas `to_dict("records")`, but no scalar sugar and the grid
    key is just another field); typed row classes (IDE-friendly, but three
    more imports, no unification, construction cost on large ingests — can be
    layered on later as sugar that produces mappings); one generic
    `write_points(table=)` replacing actuals/predictors (dispatches on role
    anyway; role-named functions read better at call sites).
  - Non-goals: a bulk COPY path (separate item if wanted).
  - Decisions taken: (a) the `(ts, {col: val})` mapping shape; (b) three
    role-named functions kept; (c) scalar sugar `(ts, 1.0)` kept.

- [x] **5. Connection handling is mixed.**
  `provision` takes a DSN and opens its own connection; reads/writes take a
  `conn`; integration classes take a DSN and connect per call.
  - Decide whether to add a thin `Store` facade bound to `(conn_or_dsn, config)`
  - Free functions stay underneath; the facade is sugar and the natural home for item 1's helpers
  - Consider letting `provision` accept an existing `conn`

  **State after items 1/2/4/6 (2026-09-02):** every SDK call is
  `f(conn, config, ...)`; `provision(dsn)` is the only function that opens a
  connection and commits; the four adapters take a DSN, connect per call,
  commit per call, and carry the lazy-config `_StoreBinding` from item 4.
  psycopg's own `with connect(dsn) as conn:` commits on normal exit and rolls
  back on exception — the facade can inherit that rather than invent rules.

  **Done (recommendations accepted, 2026-09-02)** — `forecast_store/store.py`,
  tests in `tests/test_store.py`. Decisions as taken:
  1. Add a facade at all? [yes — thin `Store`, forwarding to the free
     functions, which stay the documented lower layer]
  2. What it binds: a connection, a DSN, or both? [connection as the core:
     `Store(conn, config=None, *, schema=None)`; plus `Store.connect(source,
     ...)` as a context manager, where `source` is a DSN string or anything
     with a `.connection()` context manager (psycopg_pool's pools, duck-typed,
     no dependency). The facade never commits; transaction ownership is
     unchanged in every form — a pool checkout's commit-on-exit is the unit
     of work. Adapters take the same `source` (DSN kept: benchmark workers
     are separate processes). Pooled hot paths should load `StoreConfig`
     once and pass it in rather than rely on the lazy load per checkout.
     Rejected: a pool-holding `Store` that checks out per method and
     auto-commits — it silently makes every call its own transaction]
  3. `provision` accepts a connection too? [yes — `provision(dsn_or_conn,
     ...)`; with a connection it runs in the caller's transaction and does
     not commit (all DDL already runs in one transaction). Stays a function,
     not a facade method: provisioning is admin, not a store operation]
  4. Config resolution? [`Store(conn)` with no config loads it from the
     store on first use; `_StoreBinding` folds into `Store`]
  5. Rebuild the adapters on it? [yes — each method becomes
     `with Store.connect(dsn, ...) as store:` and caches `store.config`
     after first use; behaviour unchanged, private plumbing deleted]
  6. Which style leads in the README? [the facade in the quickstart; one
     sentence pointing at the free functions beneath]
  7. Coupling with item 10: a `store.forecast_asof(...)` method naturally
     executes and returns rows. [decide now: the facade executes; the
     `(sql, params)` builder stays for SQL users]
  8. Name? [`Store` — `from forecast_store import Store`]
  - Non-goals: shipping a pool (accepting the caller's is in scope, above),
    an async variant (can mirror later), retries.

## Second slice

- [x] **7. Exceptions have no common base.**
  `MisalignedTimestamp` and `ConflictingBelief` subclassed `ValueError`;
  `UnknownSeries`, `UnknownTable`, `MigrationRequired` subclassed `Exception`;
  undeclared columns raised plain `ValueError`.
  - [x] `ForecastStoreError` base in new `forecast_store/errors.py`; every SDK error derives from it and keeps the built-in a caller would expect as a co-base (`LookupError` for unknown series/table, `ValueError` for refused requests)
  - [x] All exceptions moved there; old import paths (`write.`, `read.`, `series.`, `provision.`) still resolve; the lazy `read` import in `write.py` is a top-level import
  - [x] Plain `ValueError`s replaced: `DeclarationMismatch` (a request contradicting a table's stored declaration — columns, scalar sugar, knowledge time, role, band) and `InvalidDeclaration` (a `StoreConfig`/binding that cannot describe a store)
  - [x] `ConflictingBelief` is no longer a `ValueError`: a conflict with stored data must not be swallowed by `except ValueError` around a write

- [x] **8. Reads return bare tuples.**
  `read_context_series` returned `(interval, [(ts, raw, value)])`;
  `StoreReader` immediately rebuilt a pandas Series from them.
  - [x] `ContextSeries` / `VersionedSeries` named tuples: still unpack as `(sample_interval, rows)`, add `.gaps` and `.to_pandas()` (UTC-canonical index/columns; pandas imported lazily, not a dependency). Both adapters use them
  - [x] `column` default documented on the read (`value` = actuals/canonical predictors; forecast logs need a band column or `mean`); the mismatch error says to pick one with `column=` (landed with item 7)

- [x] **9. Naive datetimes are not rejected.**
  `_check_grid` called `.timestamp()`, which treats naive datetimes as local
  time; Postgres casts naive values by session timezone. Silent-wrong path.
  - [x] New `forecast_store/timestamps.py`: `aware(value, name)` raises `NaiveTimestamp` (a `ForecastStoreError` + `ValueError`) or `TypeError` for a non-datetime; the grid check moved there too
  - [x] Enforced before any statement runs on every write (`target_time`, per-point `available_at` / `target_time_observed`, call-level `available_at`, run `context_start`/`context_end`), every read (`start`, `end`, `asof`, `recorded_before`), and the `forecast_asof` builder — reads had the same silent-wrong path

- [x] **10. `forecast_asof` sits at a different level than the reads.**
  Returned `(sql, params)` instead of executing, hardcoded the canonical
  `forecasts` table, and lacked `recorded_before`.
  - [x] `table=` (resolved DB-free from the config via `table_configs`), `recorded_before=`, `run_name=` pin, and name-or-id on the builder; `forecast_asof_columns()` names the row positions
  - [x] Decided with item 5: `Store.forecast_asof(...)` executes and returns rows; the `(sql, params)` builder stays for SQL users
  - [x] `Store.forecast_asof` returns `ForecastsAsOf` — `(columns, rows)` + `.to_pandas()` — mirroring item 8

  **Done (2026-09-02) as planned:** `table="forecasts"` resolved DB-free from
  the config via `table_configs` (unknown → `UnknownTable`, no run provenance
  → `DeclarationMismatch`; value columns from that declaration);
  `recorded_before` as a `recorded_at` predicate; a name keeps
  `get_series_id(%s)` in the SQL, an id becomes a direct predicate; the
  `Store` method returns a small result type with `.columns`, `.rows`,
  `.to_pandas()` (mirrors item 8); optional `run_name` pin for parity with
  the context read. Existing shape test in `tests/test_ddl.py` stays valid
  for the default case. Decisions: run_name pin [yes], typed result [yes],
  name-or-id [yes].

- [x] **12. The canonical/extra table split is a `StoreConfig` artifact.** *(done before 11)*
  Two ways to declare a forecast log (store-level `quantile_band`/`has_mean`
  vs `ForecastLogSpec`), likewise actuals; canonical names are reserved;
  "extra" is a second-class concept. Yet the persisted `store_tables` rows are
  flat (no canonical marker), the DDL emits every instance identically, and
  the split lives in three lines of `ddl._instances` plus the callback's band
  check; every other canonical name in `src` is a `table=` default.

  **Done (2026-09-02, recommendations accepted):** one flat set —
  `StoreConfig(tables=(ForecastLogSpec(...), PredictorLogSpec(...), ActualsSpec(...)))`;
  `StoreConfig()` declares the conventional trio; `StoreConfig.standard(band=,
  has_mean=, actuals_revisions=, ...)` tunes it. Per-table options live on the
  spec (incl. `value_columns`); `config.table(name)` accessor; store-level
  fields `quantile_band`/`has_mean`/`actuals_revisions`/`value_columns` go.
  Reserved names shrink to infrastructure (`series`, `runs`, `store_tables`,
  `evaluation_*`). `ddl._instances` becomes one loop; `config_from_tables`
  loses its special cases; `provision.already_provisioned` = "a store exists
  here". Adapters gain table params: callback `table=` (band validated against
  that table), reader `target_table=` (the small fix), provider measurement /
  predictor table names. CLI shortcuts stay as standard-trio options.
  Persisted rows, DDL and drift check are byte-identical → no migration.
  ~32 test call sites, mechanical. Decisions: keep spec class names [yes];
  `standard()` replaces `from_levels` [yes]; require ≥1 table [yes]; do
  before 11 [yes].

- [x] **13. The forecast-log role is named for origin, not provenance.** *(done before 11)*
  The persisted role is `own_forecasts` — "ours, not the vendors'" — while
  the spec's rule (§6.2) is "provenance, not origin": a run-bearing external
  forecast belongs in the forecast log, our own bare vintages in `predictors`.
  The other two roles (`actuals`, `predictors`) are parallel and role-shaped;
  this one is the odd one out, and item 11 would put it into hand-written
  files.
  - [x] Renamed the role to `forecasts` (parallel to the other two, matches the default table name and `ForecastLogSpec`)
  - [x] Generator role string, sweep role list, `config_from_tables` (a store still carrying `own_forecasts` gets an error that quotes the migration), class docstring, spec example, skill references
  - [x] Existing stores: one statement, in the commit message and applied to the test store —
    `UPDATE forecast.store_tables SET config = jsonb_set(config, '{role}', '"forecasts"') WHERE config->>'role' = 'own_forecasts';`
    then re-provision to regenerate the sweep

- [ ] **11. CLI cannot express the full config.**
  No `extra_tables`, `enforcement`, or `append_only_guard`; no config-file input;
  no way to register a series without hand-written SQL.
  - Add `--config path.toml` (or YAML) to `ddl` and `provision`
  - Add `register-series` subcommand
  - Add `describe` subcommand that loads the stored declaration (pairs with item 4) and reports drift

  **Plan (2026-09-02, awaiting go) — YAML only:** the file is the flat model —
  store-level `schema`/`enforcement`/`append_only_guard` plus a `tables` list
  with `name`, `role` (persisted vocabulary: `own_forecasts` | `predictors` |
  `actuals`) and the role's options; levels as floats or strings (canonicalized
  to exact Decimals). YAML fits the audience (OpenSTEF targets are YAML,
  openstef-beam requires PyYAML, the repo's `foundation` extra and ingest
  script already use it) and `describe` emits with `yaml.safe_dump(...,
  sort_keys=False)` — no emitter to write. `StoreConfig.from_dict`/`to_dict`
  stay format-agnostic. Guard the YAML footgun: a bare `on`/`yes`/`no` table
  name parses as a bool → reject non-string names with a "quote it" message.
  `--config` excludes the trio flags; `--schema` may override the file.
  `register-series NAME --interval ... [--timezone --unit --description
  --metadata JSON]` prints the id. `describe --dsn` prints the store's
  declaration as re-provisionable YAML (header: convention version,
  TimescaleDB, series count); `describe --config FILE` is a drift check (per
  table: differs / missing from store / not in file; store switches), exit 1
  on drift, sharing one comparison helper with `provision`. Two commits: file
  format + `--config`; then the two subcommands.
  Open: PyYAML as a core dependency [recommended] vs lazily imported under a
  `cli` extra.

## Small fixes

- [x] `write_forecast_run` has no return annotation (returns run_id UUID) — done alongside item 2
- [ ] `Observed` docstring describes `StoreReader` plain-string behavior, not its own
- [x] `StoreReader.context` hardcodes `table="actuals"` for the target; add a `target_table` parameter so a multi-instance store can serve the target from a second actuals instance — done with item 12 (the callback gained `table=` and the provider `measurement_table`/`predictor_table` alongside)
