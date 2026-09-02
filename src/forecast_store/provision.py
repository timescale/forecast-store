"""Provision a forecast store from its declaration.

Idempotent: re-running with the same StoreConfig is a no-op. Re-running with
a *different* declaration than the one recorded in ``store_tables`` raises
:class:`MigrationRequired` — changing a provisioned store is an explicit
migration (spec §7.3), never a side effect of provisioning.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from forecast_store.config import StoreConfig
from forecast_store.ddl import generate_ddl, hypertable_ddl, table_configs
from forecast_store.errors import MigrationRequired, NotProvisioned

__all__ = ["MigrationRequired", "NotProvisioned", "ProvisionReport", "provision"]


@dataclass(frozen=True)
class ProvisionReport:
    schema: str
    statements_executed: int
    timescaledb: bool
    already_provisioned: bool


def _store_exists(cur, schema: str) -> bool:
    # Referencing a missing relation is a parse-time error, so store_tables
    # cannot be queried until it is known to exist.
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.store_tables",))
    return cur.fetchone()[0] is not None


def _stored_config(cur, schema: str, table: str):
    cur.execute(
        f"SELECT config FROM {schema}.store_tables WHERE table_name = %s", (table,)
    )
    row = cur.fetchone()
    return row[0] if row else None


def provision(
    target: str | Any,
    config: StoreConfig | None = None,
    *,
    timescale: bool | None = None,
) -> ProvisionReport:
    """Create (or verify) the store described by ``config`` at ``target``.

    ``target`` is a DSN or a pool (a connection is opened for the job and the
    result committed), or an open connection — then the statements run in
    the caller's transaction and nothing is committed: the caller decides.

    ``timescale``: force TimescaleDB features on/off; default auto-detects
    the extension and degrades gracefully to plain Postgres.
    """
    import psycopg

    config = config or StoreConfig()
    if isinstance(target, psycopg.Connection):
        return _provision(target, config, timescale)

    from forecast_store.store import _connection

    with _connection(target) as conn:
        report = _provision(conn, config, timescale)
        conn.commit()
    return report


def _provision(conn: Any, config: StoreConfig, timescale: bool | None) -> ProvisionReport:
    with conn.cursor() as cur:
        if timescale is None:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
            timescale = cur.fetchone() is not None

        already = _store_exists(cur, config.schema)  # a store, whatever its tables
        for table, requested in table_configs(config).items():
            stored = _stored_config(cur, config.schema, table) if already else None
            if stored is not None and stored != requested:
                raise MigrationRequired(
                    f"table '{config.schema}.{table}' is provisioned with a "
                    f"different declaration.\n  stored:    {stored}\n"
                    f"  requested: {requested}\n"
                    "Changing a provisioned table (e.g. its band) is a migration; "
                    "v0 does not apply migrations. (Stored tables absent from this "
                    "config are left untouched: instances arrive as additions.)"
                )

        statements = generate_ddl(config)
        if timescale:
            statements += hypertable_ddl(config)
        for stmt in statements:
            cur.execute(stmt)

    return ProvisionReport(
        schema=config.schema,
        statements_executed=len(statements),
        timescaledb=bool(timescale),
        already_provisioned=already,
    )
