"""forecast-store: the forecast store.

Schema convention, generator, and SDK for operating forecasts on
Postgres/TimescaleDB. The convention is specified in
docs/forecast-store-convention.md; this package is its generator and
reference implementation.

The supported SDK surface is re-exported here: declare (or load) a
``StoreConfig``, ``provision`` a store, then work through a ``Store`` bound
to a connection — or through the module functions beneath it, which take
the same caller-owned psycopg connection and declaration. Engine
integrations (``forecast_store.integrations.*``) are not re-exported: each
imports its engine, so install the matching extra and import the submodule.
"""

from forecast_store.config import (
    CONVENTION_VERSION,
    LIANDER_BAND,
    ActualsSpec,
    ForecastLogSpec,
    PredictorLogSpec,
    StoreConfig,
    TableSpec,
)
from forecast_store.ddl import generate_ddl, hypertable_ddl, table_configs
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
from forecast_store.naming import parse_quantile_column, quantile_column
from forecast_store.provision import ProvisionReport, provision
from forecast_store.queries import forecast_asof
from forecast_store.read import (
    ContextSeries,
    VersionedSeries,
    read_context_series,
    read_versioned_series,
)
from forecast_store.series import SeriesRef, get_series_id, register_series
from forecast_store.store import ConnectionSource, Store
from forecast_store.write import Point, write_actuals, write_forecast_run, write_predictors

__version__ = "0.0.1.dev0"

__all__ = [
    # declaration
    "CONVENTION_VERSION",
    "LIANDER_BAND",
    "StoreConfig",
    "ForecastLogSpec",
    "PredictorLogSpec",
    "ActualsSpec",
    "TableSpec",
    # schema generation and provisioning
    "generate_ddl",
    "hypertable_ddl",
    "table_configs",
    "provision",
    "ProvisionReport",
    # the facade
    "Store",
    "ConnectionSource",
    # series registry
    "register_series",
    "get_series_id",
    "SeriesRef",
    # writes
    "write_forecast_run",
    "write_actuals",
    "write_predictors",
    "Point",
    # reads and queries
    "read_context_series",
    "read_versioned_series",
    "ContextSeries",
    "VersionedSeries",
    "forecast_asof",
    # naming
    "quantile_column",
    "parse_quantile_column",
    # errors — all under ForecastStoreError
    "ForecastStoreError",
    "ConflictingBelief",
    "DeclarationMismatch",
    "InvalidDeclaration",
    "MigrationRequired",
    "MisalignedTimestamp",
    "NaiveTimestamp",
    "NotProvisioned",
    "UnknownSeries",
    "UnknownTable",
]
