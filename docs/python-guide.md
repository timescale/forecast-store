# Using the Python library

The forecast store is a Postgres/TimescaleDB schema first — the contract is
[`forecast-store-convention.md`](forecast-store-convention.md) — and this package is its
generator plus a thin, honest SDK over it. This guide walks the store's lifecycle: connect,
declare, register, write, read, operate. Along the way it states the rules that keep a
store honest, which otherwise live only in docstrings.

Every `python` block below is executed by the test suite, in order, against a live store
(`tests/test_python_guide.py`), so what you read is what runs. Blocks marked
`# not executed` need an optional extra. Throughout, `DSN` is your connection string:

```python
import os
from datetime import datetime, timedelta, timezone

DSN = os.environ["FORECAST_STORE_DSN"]
```

## 1. Connect

`Store` binds a connection and a declaration. Every method forwards to the module function
of the same name with `(conn, config)` prepended, and nothing on it ever commits.

```python
from forecast_store import Store

with Store.connect(DSN) as store:          # opens a connection; the block is the unit of work
    print(store.schema, store.config.table_names)
# leaving the block committed; an exception would have rolled back
```

- **The block is the transaction.** `Store.connect` inherits psycopg's contract — commit on
  normal exit, rollback on exception — so group what must land together in one block.
- **A connection you already hold**, or a pool checkout, binds with `Store(conn, config)`.
  Then commit and rollback are yours.
- **Pools work by duck typing:** any object with a `.connection()` context manager, such as
  psycopg_pool's, is a valid source. No dependency is added.
- **The declaration comes from the store** when you don't pass one (`StoreConfig.from_store`),
  read once per `Store`. On a hot path with a pool, load it once and pass it in:

```python
# not executed here: needs psycopg_pool
from psycopg_pool import ConnectionPool
from forecast_store import StoreConfig

pool = ConnectionPool(DSN, min_size=1, max_size=8)
with pool.connection() as conn:
    config = StoreConfig.from_store(conn)      # once, at startup

with Store.connect(pool, config) as store:     # one checkout = one unit of work
    ...
```

The module functions beneath the facade — `register_series(conn, config, ...)`,
`write_actuals(conn, config, ...)`, `read_context_series(conn, config, ...)` and the rest —
take the same connection and declaration explicitly. Use them when you'd rather not hold a
`Store`; the adapters in `forecast_store.integrations` are built on the facade.

## 2. Declare and provision

A store is one flat set of tables. `StoreConfig()` declares the convention's trio —
`forecasts` (a forecast log: run provenance and a quantile band), `predictors` (external
forecast feeds, one row per vintage) and `actuals` (measurements) — and every `table=`
argument in the SDK defaults to those names. Nothing about them is special beyond being the
default: add tables, rename them, leave them out.

```python
from forecast_store import ActualsSpec, ForecastLogSpec, StoreConfig, provision

default = StoreConfig()                                  # the trio, Liander 7-level band
tuned = StoreConfig.standard(["0.1", "0.5", "0.9"], actuals_revisions=False)
custom = StoreConfig(
    tables=(ActualsSpec("meters"), ForecastLogSpec("day_ahead", quantile_band=["0.5"]))
)

print(default.table("forecasts").value_columns)          # ('mean', 'q05', ..., 'q95')
print(tuned.table("actuals").revisions, custom.table_names)   # False ('day_ahead', 'meters')
```

`provision` creates what is missing and verifies what exists. Re-running with the same
declaration is a no-op. Adding a table is additive. Changing a provisioned table raises
`MigrationRequired`: a migration is a decision, never a side effect of provisioning.

```python
report = provision(DSN, default)            # a DSN: opens a connection, commits
print(report.already_provisioned, report.timescaledb)

workspace = default.with_tables(
    ForecastLogSpec("guide_workspace", quantile_band=["0.1", "0.5", "0.9"], has_mean=False)
)
provision(DSN, workspace)                   # additive: declares the new instance, touches nothing else
```

Pass an open connection instead of a DSN and the DDL runs in your transaction; you decide
whether to commit. Declarations are plain data too: `forecast_store.declaration` reads and
writes them as YAML, which is what the CLI's `--config` takes and `describe` prints.

```python
from forecast_store.declaration import dumps, loads

text = dumps(workspace)
assert loads(text) == workspace
print(text.splitlines()[0])                 # schema: forecast
```

## 3. Register series

A series has a name (`site42/load`), a sample interval — the bucket grid every target time
must sit on — and optional metadata. Registration is get-or-create. Every SDK call that
takes a series accepts its name or its id.

```python
LOAD, TEMP = "guide/site42/load", "guide/site42/wx/temperature"

with Store.connect(DSN) as store:
    load_id = store.register_series(LOAD, "15 minutes", unit="MW", metadata={"source": "guide"})
    store.register_series(TEMP, timedelta(minutes=15))
    assert store.get_series_id(LOAD) == load_id
```

## 4. Write

One point shape on every table: `(target_time, values)`, where `values` maps declared
columns to numbers — or is a bare number when the table has exactly one value column. What
differs between tables is the **knowledge time**, `available_at`, and the table's role
decides how it resolves.

```python
t0 = datetime(2025, 3, 1, tzinfo=timezone.utc)
step = timedelta(minutes=15)
horizon = [t0 + i * step for i in range(8)]

with Store.connect(DSN) as store:
    # Actuals are measurements. An unstated knowledge time is arrival, measured by the
    # store; a stated one is a claim (a backfill), per batch or per row.
    store.write_actuals(LOAD, [(t0 - step, 41.0)])                                  # measured now
    store.write_actuals(LOAD, [(t0 - 3 * step, 39.5), (t0 - 2 * step, 40.2)],
                        available_at=t0 - step)                                     # batch claim
    store.write_actuals(LOAD, [(t0 - 4 * step, {"value": 38.9, "available_at": t0 - 3 * step})])

    # Predictors are external vintages. Publication time must be stated: per batch or per row.
    store.write_predictors(TEMP, [(ts, 9.5) for ts in horizon], available_at=t0 - timedelta(hours=6))

    # A forecast is one run — one knowledge time, run provenance — with many points.
    # Compute bounds are measured, never stated by arithmetic: clock reads around
    # the inference. This example backfills a simulated knowledge time, so the
    # real-wall-clock bounds diverge from available_at — the honest backfill shape.
    started_at = datetime.now(timezone.utc)
    points = [(ts, {"q10": 40.0, "q50": 42.0, "q90": 44.0}) for ts in horizon]  # the "model"
    finished_at = datetime.now(timezone.utc)
    run_id = store.write_forecast(
        series=LOAD, model="guide-model", run_name="guide/site42",
        available_at=t0 - timedelta(hours=1),
        started_at=started_at, finished_at=finished_at,
        points=points,
        params={"features": ["temperature"]},
    )
    # ...or into a workspace instance with its own band, by table name.
    store.write_forecast(
        series=LOAD, table="guide_workspace", model="guide-backtest", run_name="guide/bt",
        available_at=t0 - timedelta(hours=1), points=[(ts, {"q50": 41.5}) for ts in horizon],
    )
print(run_id)
```

- **Knowledge time by role.** A forecast log takes it from the run; a per-point
  `available_at` is refused. Actuals: per-point, else the call-level value, else the column
  default measures arrival. Predictors: per-point, else call-level, else refused —
  publication is a claim and is never defaulted.
- **Timestamps are aware.** A naive datetime is refused before any statement runs
  (`NaiveTimestamp`); target times must sit on the series' grid (`MisalignedTimestamp`).
- **Columns are declared.** An undeclared column, a bare number where the column is
  ambiguous, or a table of the wrong role is refused before any row lands
  (`DeclarationMismatch`).
- **Compute bounds are measured claims.** `started_at`/`finished_at` bound the inference
  that produced the run — stated when measured, NULL otherwise, never defaulted;
  `finished_at >= started_at` is enforced in schema. Under a release gate or backfill they
  diverge from `available_at`; `ForecastStoreCallback` fills them automatically in live
  operation.
- **Writes are idempotent.** A repeated row is a no-op. On a single-belief actuals instance a
  *different* value for a stored target raises `ConflictingBelief`, which is deliberately
  not a `ValueError`: `except ValueError` around a write must not swallow it.
- **`points` may be any iterable.** `df.to_dict("index").items()` feeds a write directly.

```python
from forecast_store import NaiveTimestamp

with Store.connect(DSN) as store:
    try:
        store.write_actuals(LOAD, [(datetime(2025, 3, 1), 1.0)])     # naive
    except NaiveTimestamp as exc:
        print(exc)
```

## 5. Read

Every read states its decision moment, `asof`, and cannot return a belief claimed after
it. Results are named tuples that still unpack as `(sample_interval, rows)` and add the
follow-ups you would write anyway.

```python
with Store.connect(DSN) as store:
    history = store.read_context_series(LOAD, table="actuals", start=t0 - 4 * step, end=t0, asof=t0)
    # The row measured *now* is not knowledge as of t0: it is filled forward and counted as a gap.
    print(history.sample_interval, history.gaps, [v for _, _, v in history.rows])

    covariate = store.read_context_series(TEMP, table="predictors", start=t0, end=t0 + 8 * step, asof=t0)
    print(covariate.to_pandas().head(2))                       # float Series on a UTC DatetimeIndex

    ours = store.read_context_series(
        LOAD, table="forecasts", column="q50", start=t0, end=t0 + 8 * step, asof=t0,
        run_name="guide/site42",
    )
    print(ours.rows[0])
```

- `column` defaults to `value`, the one value column of actuals and canonical predictors. A
  forecast log has none, so name a band column or `mean`.
- `run_name` pins the producing job on a forecast log; otherwise the latest vintage across
  all producers wins.
- `recorded_before` is the system-clock pin: pass it to freeze a read against later writes —
  backtests, evaluation, reproducing a past read.
- `read_versioned_series` is the full belief log, every vintage, for engines that apply
  their own cutoffs; `.to_pandas()` gives a UTC-canonical frame.
- `forecast_asof` is the canonical latest-vintage-per-target query: executed on a `Store`, or
  as `(sql, params)` from `forecast_store.queries` for hand-written SQL, with the same pins.

```python
with Store.connect(DSN) as store:
    latest = store.forecast_asof(LOAD, t0, t0 + 8 * step, asof=t0)
    print(latest.columns[:4])                    # ('series_id', 'target_time', 'available_at', 'run_id')
    print(latest.to_pandas()[["target_time", "q50"]].head(2))

    interval, rows = store.read_versioned_series(LOAD, table="actuals", start=t0 - 4 * step, end=t0)
    print(len(rows), "actuals vintages, no cutoff")
```

## 6. Operate

The store carries its own declaration, so a client never has to redeclare it — and can check
a declaration against it.

```python
import psycopg

from forecast_store import compare_declarations, stored_declarations

with psycopg.connect(DSN) as conn:
    live = StoreConfig.from_store(conn)                  # what the store was provisioned with
    drift = compare_declarations(stored_declarations(conn), default)
print("guide_workspace" in live.table_names)
print("differs:", sorted(drift.differs), "missing:", drift.missing, "unmanaged:", drift.unmanaged)
```

`differs` is what `provision` would refuse; `missing` is what it would add; `unmanaged`
tables are in the store, not in the declaration, and are left alone. The same check is a
shell command with an exit code, for CI:

```bash
forecast-store describe --dsn "$FORECAST_STORE_DSN"                      # the declaration, as YAML
forecast-store describe --dsn "$FORECAST_STORE_DSN" --config store.yaml  # drift check: exit 1 on drift
forecast-store register-series site42/load --interval "15 minutes" --dsn "$FORECAST_STORE_DSN"
```

Every error the SDK raises derives from `ForecastStoreError`. Each also keeps the built-in
you would have expected — `LookupError` for an unknown series or table, `ValueError` for a
refused request — so existing handlers keep working; `ConflictingBelief` is the one
deliberate exception, above.

## 7. OpenSTEF

The adapters in `forecast_store.integrations` take the same connection source as `Store`
(a DSN or a pool) and read the declaration from the store: `StoreReader` assembles a
leakage-free model input as of the decision moment, `ForecastStoreCallback` persists every
prediction with its real knowledge time, and the two openstef-beam adapters serve backtests
from the store and write their results back. The end-to-end walk-through is
[`tutorials/openstef-on-timescaledb.md`](tutorials/openstef-on-timescaledb.md); the seam-by-seam
mapping is [`integrations/openstef.md`](integrations/openstef.md).

```python
# not executed here: needs the openstef extra
from forecast_store.integrations.openstef import ForecastStoreCallback, StoreReader

dataset = StoreReader(DSN).context(
    target_series="site42/load",
    covariates={"temperature": "site42/wx/temperature"},   # a plain name reads a vendor feed
    history_start=asof - timedelta(days=30), asof=asof, horizon_end=asof + timedelta(days=3),
)
workflow.callbacks.append(ForecastStoreCallback(DSN, "site42/load"))
```
