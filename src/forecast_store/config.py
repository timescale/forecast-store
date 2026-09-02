"""Store configuration: the declaration the generator works from.

Per the convention (spec §4.4, §5.2), a store declares its tables — each
with its role and options — and every schema object is generated from that
declaration; the declaration itself is persisted in ``store_tables`` so any
client can reconstruct the store's shape from the store alone.

A store is one flat set of points tables. The convention's three canonical
names — ``forecasts``, ``predictors``, ``actuals`` — are the *default*
declaration (:func:`standard_tables`, what ``StoreConfig()`` declares) and
the defaults every ``table=`` argument in the SDK points at; they are not a
separate class of table. Any table can be added, renamed, or left out: a
store without a ``forecasts`` table is legal, and a default write into it
fails with :class:`~forecast_store.errors.UnknownTable`. Only the
infrastructure names (``series``, ``runs``, ``store_tables``, the
``evaluation_*`` tables) are reserved.
"""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Literal

from forecast_store.errors import InvalidDeclaration, UnknownTable
from forecast_store.naming import _as_level, quantile_column

CONVENTION_VERSION = "0.4.0"

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

#: The liander2024 benchmark band (7 levels) — the spike's reference instantiation.
LIANDER_BAND: tuple[Decimal, ...] = tuple(
    Decimal(s) for s in ("0.05", "0.1", "0.3", "0.5", "0.7", "0.9", "0.95")
)

#: Infrastructure tables a points table may not be named after.
RESERVED_TABLES = frozenset(
    {"series", "store_tables", "runs", "evaluation_runs", "evaluation_series", "evaluation_metrics"}
)


def band_columns(band: tuple[Decimal, ...], has_mean: bool) -> tuple[str, ...]:
    """Value columns for a forecast-log instance (mean first, then the band)."""
    cols = ("mean",) if has_mean else ()
    return cols + tuple(quantile_column(q) for q in band)


def _levels(band: Iterable[float | str | Decimal]) -> tuple[Decimal, ...]:
    """A band, canonicalized: exact Decimals, sorted, de-duplicated."""
    try:
        return tuple(sorted({_as_level(q) for q in band}))
    except ValueError as exc:
        raise InvalidDeclaration(str(exc)) from None


def _check_name(name: str) -> None:
    if not _IDENT_RE.match(name):
        raise InvalidDeclaration(f"table name must be a plain lowercase identifier: {name!r}")
    if name in RESERVED_TABLES:
        raise InvalidDeclaration(f"table name {name!r} is reserved for the store's infrastructure")


@dataclass(frozen=True)
class ForecastLogSpec:
    """A forecast log (spec §7.2, role ``forecasts``): forecasts that carry
    run provenance — value columns from its band (and ``mean``), its own
    retention/compression policies, sharing ``runs`` with every other log.
    ``forecasts`` is the conventional one; a backtest workspace kept apart
    from production history is the prototypical second. Band levels may be
    given as str/float/Decimal and are canonicalized."""

    name: str
    quantile_band: tuple[Decimal, ...] = LIANDER_BAND
    has_mean: bool = True

    def __post_init__(self) -> None:
        _check_name(self.name)
        object.__setattr__(self, "quantile_band", _levels(self.quantile_band))
        if not self.quantile_band and not self.has_mean:
            raise InvalidDeclaration(
                f"{self.name!r} must have at least one value column (band or mean)"
            )

    @property
    def value_columns(self) -> tuple[str, ...]:
        """``mean`` first (if declared), then the band's q-columns."""
        return band_columns(self.quantile_band, self.has_mean)


@dataclass(frozen=True)
class PredictorLogSpec:
    """A predictors table (spec §6.2, role ``predictors``): external forecast
    feeds, one row per vintage. ``predictors`` is the conventional one;
    vendor feeds with their own retention, or tenancy, are further ones.

    Same *shape* as Tier-2 actuals but a different *contract*, which is why
    it is not an :class:`ActualsSpec` under another name: ``available_at``
    has **no default** (a predictor row is a claim about vendor publication —
    writers must state it; a default would silently conflate publication
    with arrival), and the declared role is what evaluation and monitoring
    dispatch on (scored *as* forecasts, watched for publication lag — never
    treated as ground truth).

    A probabilistic vendor may declare a ``quantile_band`` — a band is an
    instance declaration available to either pattern; run provenance, not the
    band, is what distinguishes a forecast log."""

    name: str
    quantile_band: tuple[Decimal, ...] = ()
    has_value: bool = True

    def __post_init__(self) -> None:
        _check_name(self.name)
        object.__setattr__(self, "quantile_band", _levels(self.quantile_band))
        if not self.quantile_band and not self.has_value:
            raise InvalidDeclaration(
                f"{self.name!r} must have at least one value column (band or value)"
            )

    @property
    def value_columns(self) -> tuple[str, ...]:
        """``value`` first (if declared), then the band's q-columns."""
        cols = ("value",) if self.has_value else ()
        return cols + tuple(quantile_column(q) for q in self.quantile_band)


@dataclass(frozen=True)
class ActualsSpec:
    """An actuals table (spec §6.1, role ``actuals``): ground truth.
    ``actuals`` is the conventional one.

    Evaluation scores against it and missing-data alarms watch it;
    ``available_at`` defaults to ``now()`` because arrival is *measured*, not
    claimed (contrast :class:`PredictorLogSpec`).

    ``revisions`` is the PK switch (spec §6.1): True keys revisions by the
    knowledge clock; False admits one belief per target (single-belief),
    where a conflicting re-delivery raises instead of revising.

    ``has_target_time_observed`` (spec §6.1) adds a nullable
    ``target_time_observed`` column for 1:1 snapped feeds — the device's
    original, unsnapped timestamp. Measurement provenance, not a fourth
    clock: no query role, never in the PK, never defaulted."""

    name: str
    revisions: bool = True
    has_target_time_observed: bool = False

    def __post_init__(self) -> None:
        _check_name(self.name)

    @property
    def value_columns(self) -> tuple[str, ...]:
        return ("value",)


TableSpec = ForecastLogSpec | PredictorLogSpec | ActualsSpec


def standard_tables(
    band: Iterable[float | str | Decimal] = LIANDER_BAND,
    *,
    has_mean: bool = True,
    actuals_revisions: bool = True,
) -> tuple[TableSpec, ...]:
    """The convention's canonical trio (spec §5): ``forecasts`` with ``band``
    (and ``mean``), ``predictors`` (one point value per vintage), and
    ``actuals`` (revisioned by default — spec §6.1). What ``StoreConfig()``
    declares."""
    return (
        ForecastLogSpec("forecasts", quantile_band=tuple(band), has_mean=has_mean),
        PredictorLogSpec("predictors"),
        ActualsSpec("actuals", revisions=actuals_revisions),
    )


@dataclass(frozen=True)
class StoreConfig:
    """Declared configuration of one forecast store: its tables, plus the
    store-level switches.

    The declaration is canonicalized on construction (bands sorted and
    de-duplicated, tables in name order), so two declarations of the same
    store compare equal however they were spelled — including one rebuilt
    from the store itself with :meth:`from_store`.
    """

    tables: tuple[TableSpec, ...] = field(default_factory=standard_tables)
    schema: str = "forecast"
    enforcement: Literal["monitor", "fk"] = "monitor"
    #: Opt-in structural append-only enforcement on revisioned points tables
    #: (spec §8); tier-1 actuals carry their belief guard unconditionally.
    append_only_guard: bool = False

    def __post_init__(self) -> None:
        if not _IDENT_RE.match(self.schema):
            raise InvalidDeclaration(
                f"schema must be a plain lowercase identifier, got {self.schema!r}"
            )
        if self.enforcement not in ("monitor", "fk"):
            raise InvalidDeclaration(
                f"enforcement must be 'monitor' or 'fk', got {self.enforcement!r}"
            )
        tables = tuple(self.tables)
        if not tables:
            raise InvalidDeclaration("a store declares at least one table")
        for spec in tables:
            if not isinstance(spec, (ForecastLogSpec, PredictorLogSpec, ActualsSpec)):
                raise InvalidDeclaration(f"not a table spec: {spec!r}")
        names = [spec.name for spec in tables]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise InvalidDeclaration(f"duplicate table names: {duplicates}")
        # Declaration order is not significant.
        object.__setattr__(self, "tables", tuple(sorted(tables, key=lambda s: s.name)))

    @classmethod
    def standard(
        cls,
        band: Iterable[float | str | Decimal] = LIANDER_BAND,
        *,
        has_mean: bool = True,
        actuals_revisions: bool = True,
        **store: Any,
    ) -> StoreConfig:
        """The canonical trio, tuned — e.g.
        ``StoreConfig.standard(["0.1", "0.5", "0.9"], actuals_revisions=False, schema="fs_prod")``.
        Remaining keywords are the store-level switches."""
        return cls(
            tables=standard_tables(band, has_mean=has_mean, actuals_revisions=actuals_revisions),
            **store,
        )

    def with_tables(self, *specs: TableSpec) -> StoreConfig:
        """A copy that also declares ``specs`` — instances arrive as additions
        (spec §7.2): ``StoreConfig().with_tables(ForecastLogSpec("bt_workspace", ...))``."""
        return dataclasses.replace(self, tables=self.tables + tuple(specs))

    def table(self, name: str) -> TableSpec:
        """The declared spec for ``name``; :class:`UnknownTable` otherwise."""
        for spec in self.tables:
            if spec.name == name:
                return spec
        raise UnknownTable(
            f"{name!r} is not a table declared by this StoreConfig "
            f"(tables: {list(self.table_names)})"
        )

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self.tables)

    @classmethod
    def from_store(cls, conn: Any, schema: str = "forecast") -> StoreConfig:
        """Rebuild a provisioned store's declaration from the store alone.

        Reads the per-table declarations ``provision`` persisted in
        ``store_tables`` (spec §5.2) and inverts them — every table, with its
        band, columns, PK switch and enforcement mode — and recovers
        ``append_only_guard`` from the catalog (its guard function exists iff
        it was declared). The result compares equal to the declaration the
        store was built from and re-provisions as a no-op, so clients never
        have to redeclare (and drift from) a store's shape.

        Raises :class:`~forecast_store.provision.NotProvisioned` when no
        store exists at ``schema``.
        """
        from forecast_store.ddl import config_from_tables
        from forecast_store.provision import NotProvisioned

        if not _IDENT_RE.match(schema):
            raise InvalidDeclaration(f"schema must be a plain lowercase identifier, got {schema!r}")
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"{schema}.store_tables",))
            if cur.fetchone()[0] is None:
                raise NotProvisioned(
                    f"no forecast store at schema {schema!r} "
                    f"({schema}.store_tables does not exist) — provision it first"
                )
            cur.execute(f"SELECT table_name, config FROM {schema}.store_tables")
            declarations = dict(cur.fetchall())
            cur.execute("SELECT to_regprocedure(%s)", (f"{schema}.append_only_guard()",))
            append_only_guard = cur.fetchone()[0] is not None
        return config_from_tables(
            declarations, schema=schema, append_only_guard=append_only_guard
        )
