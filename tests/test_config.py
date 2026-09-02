"""One flat set of tables (DX item 12): the canonical trio is the default
declaration, not a separate class of table."""

from decimal import Decimal

import pytest

from forecast_store import (
    LIANDER_BAND,
    ActualsSpec,
    ForecastLogSpec,
    InvalidDeclaration,
    PredictorLogSpec,
    StoreConfig,
    UnknownTable,
    standard_tables,
)
from forecast_store.ddl import config_from_tables, table_configs

LIANDER_COLUMNS = ("q05", "q10", "q30", "q50", "q70", "q90", "q95")


def test_default_is_the_standard_trio():
    cfg = StoreConfig()
    assert cfg.table_names == ("actuals", "forecasts", "predictors")  # name order
    assert cfg == StoreConfig(tables=standard_tables()) == StoreConfig.standard()
    assert cfg.table("forecasts") == ForecastLogSpec("forecasts", LIANDER_BAND, has_mean=True)
    assert cfg.table("forecasts").value_columns == ("mean",) + LIANDER_COLUMNS
    assert cfg.table("predictors").value_columns == ("value",)
    assert cfg.table("actuals").value_columns == ("value",)
    assert cfg.table("actuals").revisions is True


def test_standard_tunes_the_trio():
    cfg = StoreConfig.standard(
        ["0.9", "0.1", 0.9], has_mean=False, actuals_revisions=False, schema="fs_x"
    )
    assert cfg.table("forecasts").quantile_band == (Decimal("0.1"), Decimal("0.9"))
    assert cfg.table("forecasts").value_columns == ("q10", "q90")
    assert cfg.table("actuals").revisions is False
    assert cfg.schema == "fs_x"


def test_any_set_of_tables_is_a_store():
    cfg = StoreConfig(
        tables=(
            ActualsSpec("meters", revisions=False),
            ForecastLogSpec("day_ahead", quantile_band=("0.5",)),
        )
    )
    assert cfg.table_names == ("day_ahead", "meters")
    with pytest.raises(UnknownTable):
        cfg.table("forecasts")  # no canonical table is implied
    # Persisted rows are per table and role-shaped, and rebuild the same declaration.
    rows = table_configs(cfg)
    assert set(rows) == {
        "day_ahead", "meters", "evaluation_runs", "evaluation_series", "evaluation_metrics",
    }
    assert config_from_tables(rows) == cfg
    # Even a canonical name is ordinary: any role may use it.
    assert StoreConfig(tables=(ActualsSpec("forecasts"),)).table("forecasts").revisions


def test_with_tables_adds_instances():
    cfg = StoreConfig().with_tables(
        ForecastLogSpec("bt_workspace", quantile_band=["0.1", "0.5", "0.9"])
    )
    assert cfg.table_names == ("actuals", "bt_workspace", "forecasts", "predictors")
    assert cfg.table("bt_workspace").value_columns == ("mean", "q10", "q50", "q90")
    assert cfg.table("forecasts") == StoreConfig().table("forecasts")  # untouched
    with pytest.raises(InvalidDeclaration, match="duplicate"):
        StoreConfig().with_tables(ActualsSpec("actuals"))


def test_declaration_rules():
    with pytest.raises(InvalidDeclaration, match="at least one table"):
        StoreConfig(tables=())
    with pytest.raises(InvalidDeclaration, match="reserved"):
        ActualsSpec("runs")
    with pytest.raises(InvalidDeclaration, match="identifier"):
        ActualsSpec("Bad-Name")
    with pytest.raises(InvalidDeclaration, match="at least one value column"):
        ForecastLogSpec("f", quantile_band=(), has_mean=False)
    with pytest.raises(InvalidDeclaration, match="at least one value column"):
        PredictorLogSpec("p", has_value=False)
    with pytest.raises(InvalidDeclaration, match="quantile level"):
        ForecastLogSpec("f", quantile_band=["1.5"])
    with pytest.raises(InvalidDeclaration, match="not a table spec"):
        StoreConfig(tables=("forecasts",))  # type: ignore[arg-type]
    with pytest.raises(InvalidDeclaration, match="no points tables"):
        config_from_tables({"evaluation_runs": {"role": "evaluation"}})


def test_former_role_name_points_at_the_migration():
    legacy = {"forecasts": {"role": "own_forecasts", "quantile_band": ["0.5"], "has_mean": True}}
    with pytest.raises(InvalidDeclaration, match="own_forecasts.*now 'forecasts'.*UPDATE"):
        config_from_tables(legacy)
