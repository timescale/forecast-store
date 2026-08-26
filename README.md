# forecast-store

The **forecast store**: TimescaleDB/Postgres as the persistence, evaluation, and
monitoring layer for any forecasting model.

Producing a forecast is nearly free; operating forecasts — keeping vintages,
guaranteeing point-in-time correctness, tracking accuracy, detecting drift — is
hand-rolled glue code everywhere. This package is the generator and reference
implementation for an open schema convention that replaces that glue.

- **Spec:** [`docs/forecast-store-convention.md`](docs/forecast-store-convention.md)
  (draft v0.2 — pre-validation)
- **Status:** Stage 1 validation spike against [OpenSTEF](https://github.com/OpenSTEF/openstef)
  4.x (LF Energy). Private while the spike runs.

## Quickstart

```bash
uv run forecast-store ddl                                # print the schema
uv run forecast-store provision --dsn postgres://...     # provision a store
uv run --extra dev pytest                                # unit tests (no DB needed)
```

Runs on any Postgres 14+; hypertables, compression, and continuous aggregates
light up automatically on TimescaleDB.
