"""Provision a forecast store from its declaration.

Idempotent: re-running with the same StoreConfig is a no-op. Re-running with
a *different* declaration than the one recorded in ``store_tables`` raises
:class:`MigrationRequired` — changing a provisioned store is an explicit
migration (spec §7.3), never a side effect of provisioning.
"""

from __future__ import annotations

from dataclasses import dataclass

from forecast_store.config import StoreConfig
from forecast_store.ddl import generate_ddl, hypertable_ddl, table_configs


class MigrationRequired(Exception):
    """The store's recorded declaration differs from the requested one."""


class NotProvisioned(Exception):
    """No forecast store exists at the given schema (no ``store_tables``)."""


@dataclass(frozen=True)
class ProvisionReport:
    schema: str
    statements_executed: int
    timescaledb: bool
    already_provisioned: bool


def _stored_config(cur, schema: str, table: str):
    # Existence check first: referencing a missing relation is a parse-time
    # error, so we cannot query store_tables until we know it exists.
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.store_tables",))
    if cur.fetchone()[0] is None:
        return None
    cur.execute(
        f"SELECT config FROM {schema}.store_tables WHERE table_name = %s", (table,)
    )
    row = cur.fetchone()
    return row[0] if row else None


def provision(
    dsn: str,
    config: StoreConfig | None = None,
    *,
    timescale: bool | None = None,
) -> ProvisionReport:
    """Create (or verify) the store described by ``config`` at ``dsn``.

    ``timescale``: force TimescaleDB features on/off; default auto-detects
    the extension and degrades gracefully to plain Postgres.
    """
    import psycopg

    config = config or StoreConfig()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            if timescale is None:
                cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
                timescale = cur.fetchone() is not None

            already = _stored_config(cur, config.schema, "forecasts")
            for table, requested in table_configs(config).items():
                stored = _stored_config(cur, config.schema, table)
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
        conn.commit()

    return ProvisionReport(
        schema=config.schema,
        statements_executed=len(statements),
        timescaledb=bool(timescale),
        already_provisioned=already is not None,
    )
