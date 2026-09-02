"""The CLI (DX item 11): declarations from a YAML file; the trio flags as
shortcuts; usage errors instead of tracebacks."""

import pytest

from forecast_store import ForecastLogSpec, StoreConfig
from forecast_store.cli import main
from forecast_store.declaration import dumps


def _write(tmp_path, config, name="store.yaml"):
    path = tmp_path / name
    path.write_text(dumps(config))
    return str(path)


def _statement(out, fragment):
    """The one DDL statement in the printed script that contains ``fragment``."""
    (stmt,) = [s for s in out.split(";\n\n") if fragment in s]
    return stmt


def test_ddl_from_a_declaration_file(tmp_path, capsys):
    cfg = StoreConfig().with_tables(
        ForecastLogSpec("bt_workspace", quantile_band=["0.5"], has_mean=False)
    )
    assert main(["ddl", "--config", _write(tmp_path, cfg), "--section", "tables"]) == 0
    workspace = _statement(capsys.readouterr().out, "CREATE TABLE IF NOT EXISTS forecast.bt_workspace")
    assert "q50 " in workspace and "mean " not in workspace


def test_schema_flag_overrides_the_file(tmp_path, capsys):
    path = _write(tmp_path, StoreConfig(schema="from_file"))
    assert main(["ddl", "--config", path, "--schema", "from_flag"]) == 0
    out = capsys.readouterr().out
    assert "from_flag.series" in out and "from_file" not in out


def test_config_excludes_the_trio_flags(tmp_path, capsys):
    path = _write(tmp_path, StoreConfig())
    with pytest.raises(SystemExit) as exit_:
        main(["ddl", "--config", path, "--band", "0.5", "--no-mean"])
    assert exit_.value.code == 2
    assert "--config declares the tables; drop --band, --no-mean" in capsys.readouterr().err


def test_bad_or_missing_file_is_a_usage_error(tmp_path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text("tables:\n  - name: x\n    role: nope\n")
    with pytest.raises(SystemExit) as exit_:
        main(["ddl", "--config", str(bad)])
    assert exit_.value.code == 2 and "role must be one of" in capsys.readouterr().err
    with pytest.raises(SystemExit) as exit_:
        main(["ddl", "--config", str(tmp_path / "missing.yaml")])
    assert exit_.value.code == 2


def test_trio_flags_still_work(capsys):
    assert main([
        "ddl", "--band", "0.1,0.9", "--no-mean", "--single-belief-actuals", "--section", "tables",
    ]) == 0
    out = capsys.readouterr().out
    forecasts = _statement(out, "CREATE TABLE IF NOT EXISTS forecast.forecasts")
    assert "q10 " in forecasts and "q90 " in forecasts and "mean " not in forecasts
    actuals = _statement(out, "CREATE TABLE IF NOT EXISTS forecast.actuals")
    assert "PRIMARY KEY (series_id, target_time)" in actuals  # single-belief


def test_provision_needs_a_dsn(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("FORECAST_STORE_DSN", raising=False)
    with pytest.raises(SystemExit) as exit_:
        main(["provision"])
    assert exit_.value.code == 2 and "FORECAST_STORE_DSN" in capsys.readouterr().err
