---
name: forecast-store
description: Provision and operate a forecast store (the forecast-store convention) on Postgres/TimescaleDB — generate/apply the schema, register series, and run the canonical as-of and evaluation queries. Use for any work against a forecast store in this repo, including the OpenSTEF validation spike.
---

# Forecast Store

The source of truth is `docs/forecast-store-convention.md` (the spec). This skill wraps
the generator and canonical queries so every store touched during development goes through
the tested code path. **Dogfooding rule: never hand-write provisioning SQL — if the
generator can't express what you need, fix the generator first, then re-provision.**

## Provision a store

```bash
# print the DDL (review before applying; default = liander 7-level band, revisioned actuals)
uv run forecast-store ddl [--band 0.05,0.1,0.3,0.5,0.7,0.9,0.95] [--no-mean] [--single-belief-actuals] [--schema forecast]
uv run forecast-store ddl --config store.yaml      # any set of tables (YAML declaration; see forecast_store.declaration)

# apply (auto-detects TimescaleDB; degrades to plain Postgres); --config works here too
uv run forecast-store provision --dsn "$FORECAST_STORE_DSN"

# register a series (get-or-create; prints the id)
uv run forecast-store register-series site42/load --interval "15 minutes" --dsn "$FORECAST_STORE_DSN"

# the store's declaration as YAML (edit, then provision --config); with --config: drift check, exit 1 on drift
uv run forecast-store describe --dsn "$FORECAST_STORE_DSN" [--config store.yaml]
```

Re-provisioning with the same declaration is a verified no-op. A *different* declaration
raises `MigrationRequired` — never force past it; band changes are explicit migrations
(spec §7.3), unsupported in v0.

## Register series

Always through the resolvers — never raw INSERTs into `series`:

```sql
SELECT forecast.register_series('mvf_gorredijk', interval '15 minutes');
SELECT forecast.get_series_id('mvf_gorredijk');  -- strict: raises on unknown names
```

Register only with explicitly known `sample_interval`; never infer it from data spacing
(spec §8). Series names are immutable machine slugs; human naming goes in `description`.

## Rules that must hold in any code you write here

- **Never write `recorded_at`** — it is system time, always the column default (spec §4.1).
- `available_at` is a writable domain claim: production writes default `now()`; backtests
  write the simulated decision moment.
- Points tables are **append-only**: a new belief is a new row, never an UPDATE.
- A run row and its points are written in **one transaction**.
- Point/mean forecasts write `mean`, never `q50`.
- Quantile columns follow the bijective naming rule — use
  `forecast_store.naming.quantile_column()` / `parse_quantile_column()`, never string-build.
- Frozen backtests pin `recorded_at <= :frozen_at` on **every** belief-log read (spec §9.2).

## Canonical queries

Use `forecast_store.queries.forecast_asof(...)` for vintage selection (returns
`(sql, params)`; `table=` for another forecast-log instance, `recorded_before=` for the
system-clock pin, `run_name=` to pin the producer) — or `Store.forecast_asof(...)`, the
executed form; the hot serving path is the generated `forecast.latest_forecasts` view.
For evaluation joins and context assembly, follow spec §9.2/§9.3 exactly — vintage
selection *before* the actuals join, and `last(value, available_at)` only inside
`GROUP BY`/cagg contexts (never as a `DISTINCT ON` substitute).

## Development

```bash
uv run --extra dev pytest          # unit tests (no database needed)
```
