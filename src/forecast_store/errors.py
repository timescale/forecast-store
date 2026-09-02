"""Every error the SDK raises on its own account, under one base.

``except ForecastStoreError`` catches anything this package decides to
refuse; the subclass says why. Each also inherits the built-in a caller
would have expected — ``LookupError`` for a name that is not registered,
``ValueError`` for a request the store cannot accept — so code written
against those keeps working. What is *not* the SDK's is raised as itself: a
psycopg error from the database, a ``TypeError`` for an argument of the
wrong kind.

One deliberate exception: :class:`ConflictingBelief` is **not** a
``ValueError``. It reports a conflict with data the store already holds, and
an ``except ValueError`` wrapped around a write to catch argument mistakes
must not swallow it.
"""

from __future__ import annotations


class ForecastStoreError(Exception):
    """Base of every error raised by forecast_store itself."""


class UnknownSeries(ForecastStoreError, LookupError):
    """The series is not registered in the store."""


class UnknownTable(ForecastStoreError, LookupError):
    """The table is not declared in ``store_tables`` (or declares no value columns)."""


class InvalidDeclaration(ForecastStoreError, ValueError):
    """A declaration that cannot describe a store: a bad identifier, an
    empty band, a reserved or duplicate table name, a schema that
    contradicts the declaration it is paired with, or stored rows that do
    not add up to one."""


class DeclarationMismatch(ForecastStoreError, ValueError):
    """A request that contradicts a table's stored declaration: an
    undeclared or unwritable column, a bare scalar where the column is
    ambiguous, a per-point knowledge time where the run supplies it (or none
    where one must be stated), a table of the wrong role, a quantile the
    store's band does not declare."""


class MisalignedTimestamp(ForecastStoreError, ValueError):
    """A target_time off the series' declared bucket grid (spec §4.1)."""


class ConflictingBelief(ForecastStoreError):
    """A single-belief table already holds a different value for this target
    (spec §6.1). Raised by the generated ``belief_guard`` trigger; identical
    re-delivery is silently idempotent instead."""


class MigrationRequired(ForecastStoreError):
    """The store's recorded declaration differs from the requested one."""


class NotProvisioned(ForecastStoreError):
    """No forecast store exists at the given schema (no ``store_tables``)."""
