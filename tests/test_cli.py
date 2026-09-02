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
    out = capsys.readouterr().out
    workspace = _statement(out, "CREATE TABLE IF NOT EXISTS forecast.bt_workspace")
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


# -- drift and the database-backed subcommands ---------------------------------

import os  # noqa: E402

from forecast_store import ActualsSpec  # noqa: E402
from forecast_store.ddl import table_configs  # noqa: E402
from forecast_store.provision import compare_declarations  # noqa: E402

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")
SERIES = "cli_smoke_series"


def test_compare_declarations():
    stored = table_configs(StoreConfig().with_tables(ActualsSpec("legacy")))
    wanted = StoreConfig.standard(["0.5"]).with_tables(ActualsSpec("brand_new"))
    drift = compare_declarations(stored, wanted)
    assert set(drift.differs) == {"forecasts"}
    assert drift.missing == ("brand_new",) and drift.unmanaged == ("legacy",)
    assert drift
    assert not compare_declarations(table_configs(StoreConfig()), StoreConfig())
    assert not compare_declarations(stored, StoreConfig())  # unmanaged alone is not drift


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_register_series_and_describe_live(tmp_path, capsys):
    psycopg = pytest.importorskip("psycopg")
    from forecast_store import provision
    from forecast_store.declaration import loads

    provision(DSN)
    _cleanup_series(psycopg)
    path = tmp_path / "store.yaml"
    try:
        # register-series: get-or-create, prints the id
        assert main([
            "register-series", SERIES, "--interval", "15 minutes", "--dsn", DSN,
            "--unit", "MW", "--metadata", '{"source": "cli"}',
        ]) == 0
        first = capsys.readouterr().out.strip()
        assert first.isdigit()
        assert main(["register-series", SERIES, "--interval", "PT15M", "--dsn", DSN]) == 0
        assert capsys.readouterr().out.strip() == first
        with pytest.raises(SystemExit) as exit_:
            main([
                "register-series", "x", "--interval", "1 hour", "--dsn", DSN, "--metadata", "[1]",
            ])
        assert exit_.value.code == 2

        # describe: the store's declaration, as a file that loads back
        assert main(["describe", "--dsn", DSN]) == 0
        out = capsys.readouterr().out
        assert out.startswith("# store 'forecast': convention 0.4.0")
        described = loads(out)
        with psycopg.connect(DSN) as conn:
            assert described == StoreConfig.from_store(conn)

        # describe --config: in sync with what it printed
        path.write_text(out)
        assert main(["describe", "--dsn", DSN, "--config", str(path)]) == 0
        assert "in sync" in capsys.readouterr().out

        # a file adding a table: missing -> drift
        path.write_text(_dumps(described.with_tables(ActualsSpec("cli_new_table"))))
        assert main(["describe", "--dsn", DSN, "--config", str(path)]) == 1
        assert "missing    cli_new_table" in capsys.readouterr().out

        # a file changing the forecasts band: differs -> drift
        changed = StoreConfig(
            tables=tuple(
                ForecastLogSpec("forecasts", quantile_band=["0.5"]) if s.name == "forecasts" else s
                for s in described.tables
            )
        )
        path.write_text(_dumps(changed))
        assert main(["describe", "--dsn", DSN, "--config", str(path)]) == 1
        assert "differs    forecasts" in capsys.readouterr().out

        # a file with fewer tables: unmanaged is reported but is not drift
        fewer = StoreConfig(tables=tuple(s for s in described.tables if s.name != "predictors"))
        path.write_text(_dumps(fewer))
        assert main(["describe", "--dsn", DSN, "--config", str(path)]) == 0
        out = capsys.readouterr().out
        assert "unmanaged  predictors" in out and "in sync" in out
    finally:
        _cleanup_series(psycopg)


def _dumps(config):
    from forecast_store.declaration import dumps

    return dumps(config)


def _cleanup_series(psycopg):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM forecast.series WHERE name = %s", (SERIES,))
