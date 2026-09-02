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

- [ ] **8. Reads return bare tuples.**
  `read_context_series` returns `(interval, [(ts, raw, value)])`;
  `StoreReader` immediately rebuilds a pandas Series from them.
  - Return a small result object (interval + rows) with a `to_pandas()` helper
  - Make the `column` default explicit in docs, or error early: `"value"` is only valid for actuals/predictors, not forecast logs

- [ ] **9. Naive datetimes are not rejected.**
  `_check_grid` calls `.timestamp()`, which treats naive datetimes as local
  time; Postgres casts naive values by session timezone. Silent-wrong path.
  - Raise on `tzinfo is None` for `target_time` and `available_at` at every write path

- [ ] **10. `forecast_asof` sits at a different level than the reads.**
  Returns `(sql, params)` instead of executing, hardcodes the canonical
  `forecasts` table, and lacks `recorded_before`.
  - Add `table` and `recorded_before` parameters
  - [x] Decided with item 5: `Store.forecast_asof(...)` executes and returns rows; the `(sql, params)` builder stays for SQL users

- [ ] **11. CLI cannot express the full config.**
  No `extra_tables`, `enforcement`, or `append_only_guard`; no config-file input;
  no way to register a series without hand-written SQL.
  - Add `--config path.toml` (or YAML) to `ddl` and `provision`
  - Add `register-series` subcommand
  - Add `describe` subcommand that loads the stored declaration (pairs with item 4) and reports drift

## Small fixes

- [x] `write_forecast_run` has no return annotation (returns run_id UUID) — done alongside item 2
- [ ] `Observed` docstring describes `StoreReader` plain-string behavior, not its own
- [ ] `StoreReader.context` hardcodes `table="actuals"` for the target; add a `target_table` parameter so a multi-instance store can serve the target from a second actuals instance
