"""forecast-store: the forecast store.

Schema convention, generator, and SDK for operating forecasts on
Postgres/TimescaleDB. The convention is specified in
docs/forecast-store-convention.md; this package is its generator and
reference implementation.

The supported SDK surface is re-exported here: declare (or load) a
``StoreConfig``, ``provision`` a store, ``register_series``, then write and
read through the functions below — all against a caller-owned psycopg
connection. Engine integrations (``forecast_store.integrations.*``) are not
re-exported: each imports its engine, so install the matching extra and
import the submodule.
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
from forecast_store.naming import parse_quantile_column, quantile_column
from forecast_store.provision import (
    MigrationRequired,
    NotProvisioned,
    ProvisionReport,
    provision,
)
from forecast_store.queries import forecast_asof
from forecast_store.read import UnknownTable, read_context_series, read_versioned_series
from forecast_store.series import SeriesRef, UnknownSeries, get_series_id, register_series
from forecast_store.write import (
    ConflictingBelief,
    MisalignedTimestamp,
    Point,
    write_actuals,
    write_forecast_run,
    write_predictors,
)

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
    "forecast_asof",
    # naming
    "quantile_column",
    "parse_quantile_column",
    # errors
    "ConflictingBelief",
    "MigrationRequired",
    "MisalignedTimestamp",
    "NotProvisioned",
    "UnknownSeries",
    "UnknownTable",
]
