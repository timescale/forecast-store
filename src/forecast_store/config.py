"""Store configuration: the declaration the generator works from.

Per the convention (spec §4.4, §5.2), a store declares its quantile band and
table options; every schema object is generated from that declaration and the
declaration itself is persisted in ``store_tables`` so any client can
reconstruct the store's shape from the store alone.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Literal

from forecast_store.naming import _as_level, quantile_column

CONVENTION_VERSION = "0.4.0"

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

#: The liander2024 benchmark band (7 levels) — the spike's reference instantiation.
LIANDER_BAND: tuple[Decimal, ...] = tuple(
    Decimal(s) for s in ("0.05", "0.1", "0.3", "0.5", "0.7", "0.9", "0.95")
)

#: Canonical table names an extra instance may not shadow.
RESERVED_TABLES = frozenset(
    {
        "series",
        "store_tables",
        "runs",
        "forecasts",
        "predictors",
        "actuals",
        "evaluation_runs",
        "evaluation_series",
        "evaluation_metrics",
    }
)


def band_columns(
    band: tuple[Decimal, ...], has_mean: bool
) -> tuple[str, ...]:
    """Value columns for a forecast-log instance (mean first, then the band)."""
    cols = ("mean",) if has_mean else ()
    return cols + tuple(quantile_column(q) for q in band)


@dataclass(frozen=True)
class ForecastLogSpec:
    """An additional pattern-2 instance (spec §7.2): its own band and value
    columns, its own retention/compression policies — sharing ``runs`` and the
    canonical read/write machinery. ``quantile_band=None`` inherits the
    store's band. The prototypical use: a backtest-workspace table kept apart
    from production forecast history."""

    name: str
    quantile_band: tuple[Decimal, ...] | None = None
    has_mean: bool = True


@dataclass(frozen=True)
class PredictorLogSpec:
    """An additional predictors-shaped instance (vendor feeds with their own
    retention, or tenancy).

    Same *shape* as Tier-2 actuals (spec §6.2) but a different *contract*,
    which is why it is not an :class:`ActualsSpec` under another name:
    ``available_at`` has **no default** (a predictor row is a claim about
    vendor publication — writers must state it; a default would silently
    conflate publication with arrival), and the declared role is what
    evaluation and monitoring dispatch on (scored *as* forecasts, watched for
    publication lag — never treated as ground truth).

    A probabilistic vendor may declare a ``quantile_band`` — a band is an
    instance declaration available to either pattern; run provenance, not the
    band, is what distinguishes a forecast log."""

    name: str
    quantile_band: tuple[Decimal, ...] = ()
    has_value: bool = True


@dataclass(frozen=True)
class ActualsSpec:
    """An additional actuals-shaped instance.

    Role ``actuals`` = ground truth: evaluation scores against it and
    missing-data alarms watch it; ``available_at`` defaults to ``now()``
    because arrival is *measured*, not claimed (contrast
    :class:`PredictorLogSpec`).

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


TableSpec = ForecastLogSpec | PredictorLogSpec | ActualsSpec


@dataclass(frozen=True)
class StoreConfig:
    """Declared configuration of one forecast store.

    The declaration is canonicalized on construction (band sorted and
    de-duplicated, ``extra_tables`` in name order), so two declarations of
    the same store compare equal however they were spelled — including one
    rebuilt from the store itself with :meth:`from_store`.
    """

    quantile_band: tuple[Decimal, ...] = field(default=LIANDER_BAND)
    schema: str = "forecast"
    has_mean: bool = True
    #: The canonical actuals PK switch (spec §6.1): True = revisioned,
    #: False = single-belief.
    actuals_revisions: bool = True
    enforcement: Literal["monitor", "fk"] = "monitor"
    #: Opt-in structural append-only enforcement on revisioned points tables
    #: (spec §8); tier-1 actuals carry their belief guard unconditionally.
    append_only_guard: bool = False
    extra_tables: tuple[TableSpec, ...] = ()

    def __post_init__(self) -> None:
        import dataclasses

        if not _IDENT_RE.match(self.schema):
            raise ValueError(f"schema must be a plain lowercase identifier, got {self.schema!r}")
        levels = tuple(sorted({_as_level(q) for q in self.quantile_band}))
        if not levels and not self.has_mean:
            raise ValueError("store must have at least one value column (band or mean)")
        object.__setattr__(self, "quantile_band", levels)
        if self.enforcement not in ("monitor", "fk"):
            raise ValueError(f"enforcement must be 'monitor' or 'fk', got {self.enforcement!r}")

        seen: set[str] = set()
        normalized: list[TableSpec] = []
        for spec in self.extra_tables:
            if not _IDENT_RE.match(spec.name):
                raise ValueError(f"table name must be a plain lowercase identifier: {spec.name!r}")
            if spec.name in RESERVED_TABLES:
                raise ValueError(f"table name {spec.name!r} shadows a canonical table")
            if spec.name in seen:
                raise ValueError(f"duplicate extra table name: {spec.name!r}")
            seen.add(spec.name)
            if isinstance(spec, ForecastLogSpec):
                band = (
                    levels
                    if spec.quantile_band is None
                    else tuple(sorted({_as_level(q) for q in spec.quantile_band}))
                )
                if not band and not spec.has_mean:
                    raise ValueError(f"{spec.name!r} must have at least one value column")
                spec = dataclasses.replace(spec, quantile_band=band)
            elif isinstance(spec, PredictorLogSpec):
                band = tuple(sorted({_as_level(q) for q in spec.quantile_band}))
                if not band and not spec.has_value:
                    raise ValueError(f"{spec.name!r} must have at least one value column")
                spec = dataclasses.replace(spec, quantile_band=band)
            normalized.append(spec)
        normalized.sort(key=lambda spec: spec.name)  # declaration order is not significant
        object.__setattr__(self, "extra_tables", tuple(normalized))

    @classmethod
    def from_levels(
        cls, levels: Iterable[float | str | Decimal], **kwargs: object
    ) -> "StoreConfig":
        return cls(quantile_band=tuple(_as_level(q) for q in levels), **kwargs)  # type: ignore[arg-type]

    @classmethod
    def from_store(cls, conn: Any, schema: str = "forecast") -> "StoreConfig":
        """Rebuild a provisioned store's declaration from the store alone.

        Reads the per-table declarations ``provision`` persisted in
        ``store_tables`` (spec §5.2) and inverts them — band, mean column,
        actuals PK switch, enforcement mode, every extra instance — and
        recovers ``append_only_guard`` from the catalog (its guard function
        exists iff it was declared). The result compares equal to the
        declaration the store was built from and re-provisions as a no-op,
        so clients never have to redeclare (and drift from) a store's shape.

        Raises :class:`~forecast_store.provision.NotProvisioned` when no
        store exists at ``schema``.
        """
        from forecast_store.ddl import config_from_tables
        from forecast_store.provision import NotProvisioned

        if not _IDENT_RE.match(schema):
            raise ValueError(f"schema must be a plain lowercase identifier, got {schema!r}")
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

    @property
    def quantile_columns(self) -> tuple[str, ...]:
        """Generated q-column names, band order."""
        return tuple(quantile_column(q) for q in self.quantile_band)

    @property
    def value_columns(self) -> tuple[str, ...]:
        """All value columns of the forecasts table (mean first, then the band)."""
        cols = ("mean",) if self.has_mean else ()
        return cols + self.quantile_columns
