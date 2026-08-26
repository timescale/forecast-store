"""forecast-store: the forecast store.

Schema convention, generator, and SDK for operating forecasts on
Postgres/TimescaleDB. The convention is specified in
docs/forecast-store-convention.md; this package is its generator and
reference implementation.
"""

from forecast_store.config import CONVENTION_VERSION, LIANDER_BAND, StoreConfig
from forecast_store.ddl import generate_ddl, hypertable_ddl
from forecast_store.naming import parse_quantile_column, quantile_column

__version__ = "0.0.1.dev0"

__all__ = [
    "CONVENTION_VERSION",
    "LIANDER_BAND",
    "StoreConfig",
    "generate_ddl",
    "hypertable_ddl",
    "parse_quantile_column",
    "quantile_column",
]
