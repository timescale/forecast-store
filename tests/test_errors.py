"""One base for every SDK-raised error (DX item 7), each keeping the
built-in a caller would have expected as a co-base."""

from datetime import datetime, timezone

import pytest

import forecast_store as fs
from forecast_store import Store, StoreConfig
from forecast_store.errors import (
    ConflictingBelief,
    DeclarationMismatch,
    ForecastStoreError,
    InvalidDeclaration,
    MigrationRequired,
    MisalignedTimestamp,
    NaiveTimestamp,
    NotProvisioned,
    UnknownSeries,
    UnknownTable,
)

ALL = (
    UnknownSeries, UnknownTable, InvalidDeclaration, DeclarationMismatch,
    MisalignedTimestamp, NaiveTimestamp, ConflictingBelief, MigrationRequired, NotProvisioned,
)


def test_every_error_shares_the_base_and_is_exported():
    for exc in ALL:
        assert issubclass(exc, ForecastStoreError)
        assert exc.__name__ in fs.__all__ and getattr(fs, exc.__name__) is exc


def test_built_in_co_bases():
    assert issubclass(UnknownSeries, LookupError) and issubclass(UnknownTable, LookupError)
    for exc in (InvalidDeclaration, DeclarationMismatch, MisalignedTimestamp, NaiveTimestamp):
        assert issubclass(exc, ValueError)
    # A conflict with stored data is not a bad argument: `except ValueError`
    # around a write must not swallow it.
    assert not issubclass(ConflictingBelief, ValueError)
    for exc in (MigrationRequired, NotProvisioned):
        assert not issubclass(exc, (ValueError, LookupError))


def test_old_import_paths_still_resolve():
    from forecast_store.provision import MigrationRequired as p1, NotProvisioned as p2
    from forecast_store.read import UnknownSeries as r1, UnknownTable as r2
    from forecast_store.series import UnknownSeries as s1
    from forecast_store.write import ConflictingBelief as w1, MisalignedTimestamp as w2

    assert (p1, p2, r1, r2, s1, w1, w2) == (
        MigrationRequired, NotProvisioned, UnknownSeries, UnknownTable,
        UnknownSeries, ConflictingBelief, MisalignedTimestamp,
    )


def test_refusals_are_typed():
    from forecast_store.ddl import config_from_tables, table_configs
    from forecast_store.write import _normalize

    with pytest.raises(InvalidDeclaration):
        StoreConfig(schema="Not-An-Identifier")
    with pytest.raises(InvalidDeclaration, match="conflicts"):
        Store(object(), StoreConfig(schema="fs_a"), schema="fs_b")
    with pytest.raises(InvalidDeclaration, match="no points tables"):
        config_from_tables({"evaluation_runs": {"role": "evaluation"}})

    decl = table_configs(StoreConfig())
    t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(DeclarationMismatch, match="not declared by"):
        _normalize("forecasts", decl["forecasts"], [(t, {"q42": 1.0})], per_point_knowledge=False)
    with pytest.raises(DeclarationMismatch, match="bare scalar"):
        _normalize("forecasts", decl["forecasts"], [(t, 1.0)], per_point_knowledge=False)
