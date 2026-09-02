"""Declaration files (DX item 11): a StoreConfig as plain data and as YAML."""

import pytest

from forecast_store import (
    ActualsSpec,
    ForecastLogSpec,
    InvalidDeclaration,
    PredictorLogSpec,
    StoreConfig,
)
from forecast_store.declaration import dumps, load, loads

WORKSPACE = StoreConfig.standard(
    ["0.1", "0.5", "0.9"], actuals_revisions=False,
    schema="fs_decl", enforcement="fk", append_only_guard=True,
).with_tables(
    ForecastLogSpec("bt_workspace", quantile_band=["0.025", "0.5", "0.975"], has_mean=False),
    PredictorLogSpec("vendor_x", quantile_band=["0.5"]),
    ActualsSpec("meters", revisions=False, has_target_time_observed=True),
)


def test_dict_round_trip_is_exact():
    data = StoreConfig().to_dict()
    assert data["tables"][1] == {
        "name": "forecasts",
        "role": "forecasts",
        "quantile_band": ["0.05", "0.1", "0.3", "0.5", "0.7", "0.9", "0.95"],
        "has_mean": True,
    }
    assert StoreConfig.from_dict(data) == StoreConfig()
    assert StoreConfig.from_dict(WORKSPACE.to_dict()) == WORKSPACE


def test_yaml_round_trip_and_readable_form():
    text = dumps(WORKSPACE)
    assert "quantile_band: [0.025, 0.5, 0.975]" in text  # numbers, inline
    assert "role: forecasts" in text and "own_forecasts" not in text
    assert loads(text) == WORKSPACE


def test_hand_written_yaml(tmp_path):
    path = tmp_path / "store.yaml"
    path.write_text(
        "schema: fs_hand\n"
        "tables:\n"
        "  - name: forecasts\n"
        "    role: forecasts\n"
        "    quantile_band: [0.1, 0.5, 0.9]\n"
        "  - {name: actuals, role: actuals}\n"
        "  - name: 'on'\n"
        "    role: predictors\n"
    )
    cfg = load(path)
    assert cfg.table_names == ("actuals", "forecasts", "on")
    assert cfg.table("forecasts").value_columns == ("mean", "q10", "q50", "q90")
    assert cfg.enforcement == "monitor" and cfg.append_only_guard is False  # defaults


def test_declaration_errors():
    with pytest.raises(InvalidDeclaration, match="'tables' list"):
        StoreConfig.from_dict({"schema": "x"})
    with pytest.raises(InvalidDeclaration, match="unknown declaration keys"):
        StoreConfig.from_dict({"tables": [], "quantile_band": []})
    with pytest.raises(InvalidDeclaration, match="role must be one of"):
        StoreConfig.from_dict({"tables": [{"name": "x", "role": "own_forecasts"}]})
    with pytest.raises(InvalidDeclaration, match="unknown keys.*revisions"):
        StoreConfig.from_dict({"tables": [{"name": "x", "role": "forecasts", "revisions": True}]})
    with pytest.raises(InvalidDeclaration, match="quote"):
        loads("tables:\n  - name: on\n    role: actuals\n")  # YAML 1.1: `on` is True
    with pytest.raises(InvalidDeclaration, match="mapping at the top level"):
        loads("- just\n- a list\n")
    with pytest.raises(InvalidDeclaration, match="not valid YAML"):
        loads("tables: [unclosed")
