import json
from decimal import Decimal

import pytest

from forecast_store.config import CONVENTION_VERSION, LIANDER_BAND, StoreConfig
from forecast_store.ddl import generate_ddl, hypertable_ddl, table_configs
from forecast_store.queries import forecast_asof

LIANDER_COLUMNS = ("q05", "q10", "q30", "q50", "q70", "q90", "q95")


@pytest.fixture()
def config():
    return StoreConfig()  # liander band, revisioned actuals, mean, monitor


def stmt_for(statements, fragment):
    matches = [s for s in statements if fragment in s]
    assert matches, f"no statement contains {fragment!r}"
    assert len(matches) == 1, f"multiple statements contain {fragment!r}"
    return matches[0]


def test_liander_band_is_default(config):
    assert config.quantile_band == LIANDER_BAND
    assert config.quantile_columns == LIANDER_COLUMNS
    assert config.value_columns == ("mean",) + LIANDER_COLUMNS


def test_band_is_sorted_and_deduplicated():
    c = StoreConfig.from_levels(["0.9", "0.1", 0.9, "0.5"])
    assert c.quantile_band == (Decimal("0.1"), Decimal("0.5"), Decimal("0.9"))


def test_forecasts_table_has_band_columns(config):
    forecasts = stmt_for(generate_ddl(config), "CREATE TABLE IF NOT EXISTS forecast.forecasts")
    for col in ("mean", *LIANDER_COLUMNS):
        assert f"{col} " in forecasts
    assert "recorded_at" in forecasts
    assert "PRIMARY KEY (series_id, target_time, run_id)" in forecasts


def test_schema_comes_first_and_seed_last(config):
    statements = generate_ddl(config)
    assert statements[0].startswith("CREATE SCHEMA IF NOT EXISTS forecast")
    assert "INSERT INTO forecast.store_tables" in statements[-1]


def test_statements_are_idempotent(config):
    for stmt in generate_ddl(config):
        assert (
            "IF NOT EXISTS" in stmt
            or "CREATE OR REPLACE" in stmt
            or "ON CONFLICT" in stmt
        ), f"non-idempotent statement:\n{stmt}"


def test_resolvers(config):
    statements = generate_ddl(config)
    strict = stmt_for(statements, "FUNCTION forecast.get_series_id")
    assert "RAISE EXCEPTION" in strict  # strict: typos fail at write time
    assert "STABLE" in strict
    goc = stmt_for(statements, "FUNCTION forecast.register_series")
    assert "ON CONFLICT (name) DO NOTHING" in goc


def test_series_name_collates_c(config):
    series = stmt_for(generate_ddl(config), "CREATE TABLE IF NOT EXISTS forecast.series ")
    assert 'COLLATE "C"' in series


def test_seed_declares_band_and_version(config):
    seed = generate_ddl(config)[-1]
    assert CONVENTION_VERSION in seed
    band_json = json.dumps([str(q) for q in LIANDER_BAND])[1:-1]  # elements only
    assert band_json in seed


def test_table_configs_cover_all_seeded_tables(config):
    assert set(table_configs(config)) == {
        "forecasts",
        "predictors",
        "actuals",
        "evaluation_runs",
        "evaluation_series",
        "evaluation_metrics",
    }


def test_single_belief_actuals_pk():
    """Revisions are the PK switch (spec §6.1): identical columns, different key."""
    statements = generate_ddl(StoreConfig(actuals_revisions=False))
    actuals = stmt_for(statements, "CREATE TABLE IF NOT EXISTS forecast.actuals")
    assert "available_at timestamptz NOT NULL DEFAULT now()" in actuals  # universal knowledge clock
    assert "PRIMARY KEY (series_id, target_time)" in actuals  # single belief per target
    # The single-belief write path is skip-or-raise via the belief guard.
    fn = stmt_for(statements, "CREATE OR REPLACE FUNCTION forecast.belief_guard")
    assert "RETURN NULL" in fn and "RAISE EXCEPTION" in fn
    assert any(
        "TRIGGER belief_guard BEFORE UPDATE ON forecast.actuals" in s for s in statements
    )


def test_revisioned_has_no_belief_guard_trigger(config):
    statements = generate_ddl(config)  # canonical: revisioned
    stmt_for(statements, "CREATE OR REPLACE FUNCTION forecast.belief_guard")  # always ships
    assert not any("TRIGGER belief_guard" in s for s in statements)


def test_append_only_guard_opt_in(config):
    assert not any("append_only_guard" in s for s in generate_ddl(config))
    guarded = generate_ddl(StoreConfig(append_only_guard=True))
    stmt_for(guarded, "CREATE OR REPLACE FUNCTION forecast.append_only_guard")
    for table in ("forecasts", "predictors", "actuals"):  # all revisioned here
        assert any(
            f"TRIGGER append_only_guard BEFORE UPDATE ON forecast.{table}" in s
            for s in guarded
        )


def test_revisioned_actuals_have_both_clocks(config):
    actuals = stmt_for(generate_ddl(config), "CREATE TABLE IF NOT EXISTS forecast.actuals")
    assert "available_at" in actuals
    assert "PRIMARY KEY (series_id, target_time, available_at)" in actuals


def test_hypertables_cover_points_tables(config):
    stmts = hypertable_ddl(config)
    # per table: convert + columnstore settings + policy; plus the sweep fn
    assert len(stmts) == 3 * 3 + 1
    for table in ("forecasts", "predictors", "actuals"):
        assert any(
            f"create_hypertable('forecast.{table}'" in s
            and "if_not_exists => TRUE" in s
            and "migrate_data => TRUE" in s  # plain-PG -> TS upgrade path
            for s in stmts
        )
        assert any(
            f"ALTER TABLE forecast.{table} SET (timescaledb.enable_columnstore" in s
            and "timescaledb.segmentby = 'series_id'" in s
            for s in stmts
        )
        assert any(
            f"add_columnstore_policy('forecast.{table}'" in s and "if_not_exists => true" in s
            for s in stmts
        )


def test_columnstore_orderby_uniform():
    # available_at is the knowledge clock on both shapes; one orderby everywhere.
    for revisions in (False, True):
        actuals = [
            s for s in hypertable_ddl(StoreConfig(actuals_revisions=revisions)) if ".actuals SET" in s
        ]
        assert "timescaledb.orderby = 'target_time DESC, available_at DESC'" in actuals[0]


def test_custom_schema_is_used_everywhere():
    statements = generate_ddl(StoreConfig(schema="fs_test"))
    joined = "\n".join(statements)
    assert "forecast." not in joined
    assert "fs_test.series" in joined


def test_forecast_asof_query(config):
    from datetime import datetime, timezone

    t0 = datetime(2024, 7, 30, tzinfo=timezone.utc)
    t1 = datetime(2024, 7, 31, tzinfo=timezone.utc)
    asof = datetime(2024, 7, 29, 6, 0, tzinfo=timezone.utc)
    sql, params = forecast_asof(config, "mvf_gorredijk", t0, t1, asof)
    assert "DISTINCT ON (f.series_id, f.target_time)" in sql
    assert "forecast.get_series_id(%s)" in sql
    assert "f.available_at <= %s" in sql
    assert params == ("mvf_gorredijk", t0, t1, asof)


def test_invalid_configs_rejected():
    with pytest.raises(ValueError):
        StoreConfig(schema="Bad-Schema")
    with pytest.raises(ValueError):
        StoreConfig(quantile_band=(), has_mean=False)
    with pytest.raises(ValueError):
        StoreConfig.from_levels(["1.5"])


def test_sweep_function_generated(config):
    # time_bucket-based, so it ships with the TimescaleDB layer — never the
    # engine-neutral DDL (a SQL function body is validated at CREATE).
    assert not any("data_quality_sweep" in s for s in generate_ddl(config))
    sweep = stmt_for(hypertable_ddl(config), "FUNCTION forecast.data_quality_sweep")
    assert "CREATE OR REPLACE" in sweep
    assert "orphan_series" in sweep
    assert "off_grid_target_time" in sweep
    assert "time_bucket(sr.sample_interval, p.target_time)" in sweep
    for table in ("forecasts", "predictors", "actuals"):
        assert f"forecast.{table} p" in sweep
    # No instance declares the observed column in the canonical config.
    assert "observed_outside_bucket" not in sweep
