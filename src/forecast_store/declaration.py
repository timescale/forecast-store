"""Declaration files: a :class:`StoreConfig` as YAML.

The file is the flat model as plain data (:meth:`StoreConfig.to_dict`) — any
set of tables, each with its role and options::

    schema: forecast
    enforcement: monitor
    append_only_guard: false
    tables:
    - name: forecasts
      role: forecasts
      quantile_band: [0.05, 0.1, 0.3, 0.5, 0.7, 0.9, 0.95]
      has_mean: true
    - name: predictors
      role: predictors
    - name: actuals
      role: actuals
      revisions: true

Roles are the persisted vocabulary (``store_tables.config->>'role'``). Band
levels may be numbers or strings; both canonicalize to exact decimals.
``forecast-store describe`` prints a store in this form and
``forecast-store provision --config`` reads it back. One YAML footgun: a
bare table name such as ``on`` or ``yes`` parses as a boolean — quote it.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import yaml

from forecast_store.config import StoreConfig
from forecast_store.errors import InvalidDeclaration

__all__ = ["dumps", "load", "loads"]


def loads(text: str) -> StoreConfig:
    """Parse a YAML declaration."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise InvalidDeclaration(f"not valid YAML: {exc}") from None
    if not isinstance(data, Mapping):
        raise InvalidDeclaration("a declaration file holds a mapping at the top level")
    return StoreConfig.from_dict(data)


def load(path: str | Path) -> StoreConfig:
    """Parse the YAML declaration at ``path``."""
    return loads(Path(path).read_text())


def dumps(config: StoreConfig) -> str:
    """Render ``config`` as a YAML declaration that :func:`loads` reads back.

    Bands are written as numbers for readability; they round-trip exactly.
    """
    data = config.to_dict()
    for table in data["tables"]:
        if "quantile_band" in table:
            table["quantile_band"] = [float(q) for q in table["quantile_band"]]
    return yaml.safe_dump(data, sort_keys=False, default_flow_style=None)
