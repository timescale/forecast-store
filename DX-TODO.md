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

- [ ] **3. Three write functions, three `points` shapes.**
  Forecasts: `(ts, {col: value})`. Actuals: `(ts, value)` or
  `(ts, value, observed)` depending on the table's stored declaration.
  Predictors: `(ts, available_at, value)`. The arity-by-declaration rule only
  fails at runtime.
  - Decide: small typed row classes vs. one keyword-dict shape across all three
  - Validate tuple arity up front with a clear error naming the table

- [ ] **5. Connection handling is mixed.**
  `provision` takes a DSN and opens its own connection; reads/writes take a
  `conn`; integration classes take a DSN and connect per call.
  - Decide whether to add a thin `Store` facade bound to `(conn_or_dsn, config)`
  - Free functions stay underneath; the facade is sugar and the natural home for item 1's helpers
  - Consider letting `provision` accept an existing `conn`

## Second slice

- [ ] **7. Exceptions have no common base.**
  `MisalignedTimestamp` and `ConflictingBelief` subclass `ValueError`;
  `UnknownSeries`, `UnknownTable`, `MigrationRequired` subclass `Exception`;
  undeclared columns raise plain `ValueError`.
  - Add `ForecastStoreError` base in a new `forecast_store/errors.py`
  - Move all exceptions there; remove the lazy `read` import from `write.py`
  - Replace the plain `ValueError` for undeclared columns with a typed error

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
  - Decide: execute like the reads, or give the reads a builder form too

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
