"""Provision a forecast store from its declaration.

Idempotent: re-running with the same StoreConfig is a no-op. Re-running with
a *different* declaration than the one recorded in ``store_tables`` raises
:class:`MigrationRequired` — changing a provisioned store is an explicit
migration (spec §7.3), never a side effect of provisioning.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from forecast_store.config import StoreConfig
from forecast_store.ddl import generate_ddl, hypertable_ddl, table_configs
from forecast_store.errors import MigrationRequired, NotProvisioned

__all__ = [
    "Drift",
    "MigrationRequired",
    "NotProvisioned",
    "ProvisionReport",
    "compare_declarations",
    "provision",
    "stored_declarations",
]


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


def _stored_rows(cur, schema: str) -> dict[str, Mapping[str, Any]]:
    cur.execute(f"SELECT table_name, config FROM {schema}.store_tables")
    return dict(cur.fetchall())


def stored_declarations(conn: Any, schema: str = "forecast") -> dict[str, Mapping[str, Any]]:
    """The store's persisted per-table declarations, ``{table_name: config}``
    (spec §5.2). Raises :class:`NotProvisioned` when no store exists at ``schema``."""
    with conn.cursor() as cur:
        if not _store_exists(cur, schema):
            raise NotProvisioned(
                f"no forecast store at schema {schema!r} ({schema}.store_tables does not exist)"
            )
        return _stored_rows(cur, schema)


@dataclass(frozen=True)
class Drift:
    """How a requested declaration relates to a store's persisted rows, table
    by table (spec §7.3). ``differs`` blocks provisioning — changing a
    provisioned table is a migration. ``missing`` tables are what provisioning
    would add. ``unmanaged`` tables are in the store but not in the request
    and are left untouched (instances arrive as additions). The object is
    truthy when the store is not exactly what the request declares —
    ``differs`` or ``missing``; ``unmanaged`` alone is not drift."""

    #: table -> (stored, requested)
    differs: Mapping[str, tuple[Mapping[str, Any], Mapping[str, Any]]]
    missing: tuple[str, ...]
    unmanaged: tuple[str, ...]

    def __bool__(self) -> bool:
        return bool(self.differs or self.missing)


def compare_declarations(
    stored: Mapping[str, Mapping[str, Any]], requested: StoreConfig
) -> Drift:
    """Compare a store's persisted rows (:func:`stored_declarations`) with a
    declaration — the one comparison ``provision`` refuses on and
    ``forecast-store describe --config`` reports."""
    wanted = table_configs(requested)
    return Drift(
        differs={
            t: (stored[t], wanted[t]) for t in wanted if t in stored and stored[t] != wanted[t]
        },
        missing=tuple(t for t in wanted if t not in stored),
        unmanaged=tuple(t for t in stored if t not in wanted),
    )


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
        drift = compare_declarations(_stored_rows(cur, config.schema) if already else {}, config)
        if drift.differs:
            detail = "".join(
                f"  {config.schema}.{table}\n    stored:    {stored}\n    requested: {requested}\n"
                for table, (stored, requested) in drift.differs.items()
            )
            raise MigrationRequired(
                f"{len(drift.differs)} table(s) are provisioned with a different declaration:\n"
                f"{detail}"
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
