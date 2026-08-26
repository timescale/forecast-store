"""The generator: StoreConfig -> DDL.

Emits the convention's schema (spec §5-§7) as an ordered list of SQL
statements. Statements are idempotent (``IF NOT EXISTS`` / ``CREATE OR
REPLACE``) so provisioning can be re-run; changing a declaration that is
already provisioned (e.g. the band) is a migration and is rejected by the
provisioner in v0, never applied silently.

Hypertable/compression statements are emitted separately: they light up on
TimescaleDB and are skipped on plain Postgres (spec §3 goal 4).
"""

from __future__ import annotations

import json

from forecast_store.naming import quantile_column
from forecast_store.config import (
    CONVENTION_VERSION,
    ActualsSpec,
    ForecastLogSpec,
    PredictorLogSpec,
    StoreConfig,
    band_columns,
)


def _instances(config: StoreConfig) -> list[dict]:
    """The store's points-table instances — canonical three plus extras.

    Single source for CREATE TABLE emission, store_tables declarations, and
    hypertable/columnstore statements, so an extra instance is identical to a
    canonical one everywhere by construction.
    """

    def forecast_instance(name: str, band, has_mean: bool) -> dict:
        return {
            "name": name,
            "role": "own_forecasts",
            "shape": "forecast",
            "band": band,
            "value_columns": list(band_columns(band, has_mean)),
            "has_mean": has_mean,
            "knowledge_column": "available_at",
            "has_runs": True,
            "orderby": "target_time DESC, available_at DESC",
        }

    def actuals_instance(name: str, revisions: bool, observed: bool = False) -> dict:
        return {
            "name": name,
            "role": "actuals",
            "shape": "actuals",
            # Revisions are the PK switch (spec §6.1): True keys revisions by
            # the knowledge clock; False admits one belief per target. Columns
            # are identical; available_at is the knowledge clock on both.
            "revisions": revisions,
            "observed": observed,
            "value_columns": ["value"],
            "knowledge_column": "available_at",
            "has_runs": False,
            "orderby": "target_time DESC, available_at DESC",
        }

    def predictor_instance(name: str, band, has_value: bool) -> dict:
        value_columns = (["value"] if has_value else []) + [
            quantile_column(q) for q in band
        ]
        return {
            "name": name,
            "role": "predictors",
            "shape": "predictor",
            "band": band,
            "value_columns": value_columns,
            "has_value": has_value,
            "knowledge_column": "available_at",
            "has_runs": False,
            "orderby": "target_time DESC, available_at DESC",
        }

    instances = [
        forecast_instance("forecasts", config.quantile_band, config.has_mean),
        predictor_instance("predictors", (), has_value=True),
        actuals_instance("actuals", config.actuals_revisions),
    ]
    for spec in config.extra_tables:
        if isinstance(spec, ForecastLogSpec):
            instances.append(forecast_instance(spec.name, spec.quantile_band, spec.has_mean))
        elif isinstance(spec, PredictorLogSpec):
            instances.append(predictor_instance(spec.name, spec.quantile_band, spec.has_value))
        elif isinstance(spec, ActualsSpec):
            instances.append(
                actuals_instance(spec.name, spec.revisions, spec.has_target_time_observed)
            )
    return instances


def _series_table(s: str) -> str:
    return f"""\
CREATE TABLE IF NOT EXISTS {s}.series (
    series_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            text COLLATE "C" UNIQUE NOT NULL,
    sample_interval interval NOT NULL,
    timezone        text,
    unit            text,
    description     text,
    metadata        jsonb,
    updated_at      timestamptz NOT NULL DEFAULT now()
)"""


def _store_tables_table(s: str) -> str:
    return f"""\
CREATE TABLE IF NOT EXISTS {s}.store_tables (
    table_name         text PRIMARY KEY,
    convention_version text NOT NULL,
    config             jsonb NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT now()
)"""


def _get_series_id_fn(s: str) -> str:
    return f"""\
CREATE OR REPLACE FUNCTION {s}.get_series_id(p_name text)
RETURNS bigint
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_id bigint;
BEGIN
    SELECT series_id INTO v_id FROM {s}.series WHERE name = p_name;
    IF v_id IS NULL THEN
        RAISE EXCEPTION 'unknown series name: %', p_name
            USING HINT = 'register it first with {s}.register_series(...)';
    END IF;
    RETURN v_id;
END
$$"""


def _register_series_fn(s: str) -> str:
    return f"""\
CREATE OR REPLACE FUNCTION {s}.register_series(
    p_name            text,
    p_sample_interval interval,
    p_timezone        text DEFAULT NULL,
    p_unit            text DEFAULT NULL,
    p_description     text DEFAULT NULL,
    p_metadata        jsonb DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    v_id bigint;
BEGIN
    INSERT INTO {s}.series (name, sample_interval, timezone, unit, description, metadata)
    VALUES (p_name, p_sample_interval, p_timezone, p_unit, p_description, p_metadata)
    ON CONFLICT (name) DO NOTHING
    RETURNING series_id INTO v_id;
    IF v_id IS NULL THEN
        SELECT series_id INTO v_id FROM {s}.series WHERE name = p_name;
    END IF;
    RETURN v_id;
END
$$"""


def _runs_table(s: str) -> str:
    return f"""\
CREATE TABLE IF NOT EXISTS {s}.runs (
    run_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_name      text,
    model         text NOT NULL,
    model_version text,
    available_at  timestamptz NOT NULL DEFAULT now(),
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    context_start timestamptz,
    context_end   timestamptz,
    params        jsonb
)"""



def _points_table(s: str, inst: dict) -> str:
    if inst["shape"] == "forecast":
        value_cols = "".join(
            f"\n    {col:<13} double precision," for col in inst["value_columns"]
        )
        return f"""\
CREATE TABLE IF NOT EXISTS {s}.{inst["name"]} (
    run_id       uuid NOT NULL,           -- no FK by default; see spec §8
    series_id    bigint NOT NULL,
    target_time  timestamptz NOT NULL,
    available_at timestamptz NOT NULL,    -- denormalized from the run (writable claim)
    recorded_at  timestamptz NOT NULL DEFAULT now(),  -- system clock (never written){value_cols}
    PRIMARY KEY (series_id, target_time, run_id)
)"""
    if inst["shape"] == "predictor":
        value_cols = "".join(
            f"\n    {col:<13} double precision," for col in inst["value_columns"]
        )
        return f"""\
CREATE TABLE IF NOT EXISTS {s}.{inst["name"]} (
    series_id    bigint NOT NULL,
    target_time  timestamptz NOT NULL,
    available_at timestamptz NOT NULL,    -- vendor publication time: stated, never defaulted
    recorded_at  timestamptz NOT NULL DEFAULT now(),  -- when the store ingested it{value_cols}
    PRIMARY KEY (series_id, target_time, available_at)
)"""
    # Nullable, never defaulted: the device's unsnapped timestamp (spec §6.1).
    observed_col = (
        "\n    target_time_observed timestamptz," if inst.get("observed") else ""
    )
    # Revisions are the PK switch (spec §6.1): identical columns, different key.
    pk = (
        "PRIMARY KEY (series_id, target_time, available_at)"
        if inst["revisions"]
        else "PRIMARY KEY (series_id, target_time)"  # single belief per target
    )
    return f"""\
CREATE TABLE IF NOT EXISTS {s}.{inst["name"]} (
    series_id    bigint NOT NULL,
    target_time  timestamptz NOT NULL,{observed_col}
    available_at timestamptz NOT NULL DEFAULT now(),
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    value        double precision,
    {pk}
)"""


def _asof_index(s: str, name: str) -> str:
    return f"""\
CREATE INDEX IF NOT EXISTS {name}_asof_idx
    ON {s}.{name} (series_id, target_time, available_at DESC)"""


def _latest_view(s: str, inst: dict) -> str:
    cols = ", ".join(f"f.{c}" for c in inst["value_columns"])
    return f"""\
CREATE OR REPLACE VIEW {s}.latest_{inst["name"]} AS
SELECT DISTINCT ON (f.series_id, f.target_time)
       s.name AS series_name,
       f.series_id, f.target_time, f.available_at, f.run_id,
       {cols}
FROM {s}.{inst["name"]} f
JOIN {s}.series s USING (series_id)
ORDER BY f.series_id, f.target_time, f.available_at DESC"""


def _evaluation_tables(s: str) -> list[str]:
    return [
        f"""\
CREATE TABLE IF NOT EXISTS {s}.evaluation_runs (
    eval_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_name    text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    params      jsonb
)""",
        f"""\
CREATE TABLE IF NOT EXISTS {s}.evaluation_series (
    eval_series_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_name  text NOT NULL,
    series_id bigint NOT NULL,
    filtering text NOT NULL,
    win       text NOT NULL,
    quantile  text NOT NULL DEFAULT 'global',
    metric    text NOT NULL,
    UNIQUE (run_name, series_id, filtering, win, quantile, metric)
)""",
        f"""\
CREATE TABLE IF NOT EXISTS {s}.evaluation_metrics (
    eval_series_id bigint NOT NULL,
    ts             timestamptz NOT NULL,
    eval_run_id    uuid NOT NULL,
    value          double precision,
    PRIMARY KEY (eval_series_id, ts, eval_run_id)
)""",
    ]


def _belief_guard_fn(s: str) -> str:
    """Write-path trigger for single-belief actuals (spec §6.1).

    The canonical single-belief write is ``INSERT .. ON CONFLICT DO UPDATE SET value
    = EXCLUDED.value``; this trigger turns the update into skip-or-raise:
    identical re-delivery is a no-op (no dead tuple), a conflicting value
    raises — the world disagreeing with the single-belief assumption is never
    silently swallowed. Verified inert under compression, decompression,
    policy jobs, and DML over compressed chunks.
    """
    return f"""\
CREATE OR REPLACE FUNCTION {s}.belief_guard() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF NEW.value IS NOT DISTINCT FROM OLD.value THEN
        RETURN NULL;  -- identical re-delivery: idempotent skip
    END IF;
    RAISE EXCEPTION 'conflicting belief for series % at %: stored %, incoming % (single-belief table, spec 6.1)',
        OLD.series_id, OLD.target_time, OLD.value, NEW.value
        USING ERRCODE = 'integrity_constraint_violation';
END $fn$"""


def _belief_guard_trigger(s: str, name: str) -> str:
    return f"""\
CREATE OR REPLACE TRIGGER belief_guard BEFORE UPDATE ON {s}.{name}
FOR EACH ROW EXECUTE FUNCTION {s}.belief_guard()"""


def _append_only_guard_fn(s: str) -> str:
    """Opt-in structural append-only enforcement for revisioned points tables
    (spec §8): no legitimate path ever UPDATEs them, so the guard always
    raises."""
    return f"""\
CREATE OR REPLACE FUNCTION {s}.append_only_guard() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    RAISE EXCEPTION 'points tables are append-only (spec 4.1): a new belief is a new row'
        USING ERRCODE = 'integrity_constraint_violation';
END $fn$"""


def _append_only_guard_trigger(s: str, name: str) -> str:
    return f"""\
CREATE OR REPLACE TRIGGER append_only_guard BEFORE UPDATE ON {s}.{name}
FOR EACH ROW EXECUTE FUNCTION {s}.append_only_guard()"""


def _sweep_fn(config: StoreConfig) -> str:
    """The §8 orphan/grid sweep, generated per store as a SQL function.

    Monitor-first enforcement's backstop: per points instance it reports
    series ids absent from the registry (recent write window), off-grid
    ``target_time`` (``time_bucket`` against the registry's declared
    ``sample_interval``; intervals of a month or longer are skipped — no fixed
    stride), and — where declared — ``target_time_observed`` outside its
    target's bucket. Uses ``time_bucket``, so it ships with the TimescaleDB
    layer (``hypertable_ddl``), not the engine-neutral DDL. Scheduling is
    deployment-owned (cron, ``add_job``).
    """
    s = config.schema
    parts: list[str] = []
    for inst in _instances(config):
        name = inst["name"]
        parts.append(f"""\
    SELECT 'orphan_series'::text, '{name}'::text, p.series_id, count(*)
    FROM {s}.{name} p LEFT JOIN {s}.series sr USING (series_id)
    WHERE p.recorded_at > now() - scan_window AND sr.series_id IS NULL
    GROUP BY p.series_id""")
        parts.append(f"""\
    SELECT 'off_grid_target_time', '{name}', p.series_id, count(*)
    FROM {s}.{name} p JOIN {s}.series sr USING (series_id)
    WHERE p.recorded_at > now() - scan_window
      AND sr.sample_interval > interval '0'
      AND sr.sample_interval < interval '28 days'
      AND time_bucket(sr.sample_interval, p.target_time) <> p.target_time
    GROUP BY p.series_id""")
        if inst.get("observed"):
            parts.append(f"""\
    SELECT 'observed_outside_bucket', '{name}', p.series_id, count(*)
    FROM {s}.{name} p JOIN {s}.series sr USING (series_id)
    WHERE p.recorded_at > now() - scan_window
      AND p.target_time_observed IS NOT NULL
      AND (p.target_time_observed < p.target_time
           OR p.target_time_observed >= p.target_time + sr.sample_interval)
    GROUP BY p.series_id""")
    body = "\n    UNION ALL\n".join(parts)
    return f"""\
CREATE OR REPLACE FUNCTION {s}.data_quality_sweep(scan_window interval DEFAULT '2 days')
RETURNS TABLE(issue text, table_name text, series_id bigint, n bigint)
LANGUAGE sql STABLE AS $$
{body}
$$"""


def table_configs(config: StoreConfig) -> dict[str, dict[str, object]]:
    """The per-table declarations persisted in store_tables (spec §5.2).

    store_tables is the read-routing registry: APIs take table names, and the
    reader resolves each table's value columns and knowledge clock from these
    declarations — never from hardcoded table semantics. Every instance
    (canonical or extra) is declared here identically, which is what makes a
    second forecast table readable the moment its row exists.
    """
    configs: dict[str, dict[str, object]] = {}
    for inst in _instances(config):
        declaration: dict[str, object] = {
            "role": inst["role"],
            "value_columns": inst["value_columns"],
            "knowledge_column": inst["knowledge_column"],
            "has_runs": inst["has_runs"],
            "enforcement": config.enforcement,
        }
        if inst["shape"] == "forecast":
            declaration["quantile_band"] = [str(q) for q in inst["band"]]
            declaration["has_mean"] = inst["has_mean"]
        elif inst["shape"] == "predictor":
            declaration["quantile_band"] = [str(q) for q in inst["band"]]
            declaration["has_value"] = inst["has_value"]
        else:
            declaration["revisions"] = inst["revisions"]
            # Only when true: absent means undeclared, and existing stored
            # declarations stay drift-free.
            if inst.get("observed"):
                declaration["has_target_time_observed"] = True
        configs[inst["name"]] = declaration
    configs["evaluation_runs"] = {"role": "evaluation"}
    configs["evaluation_series"] = {"role": "evaluation"}
    configs["evaluation_metrics"] = {"role": "evaluation"}
    return configs


def _seed_store_tables(config: StoreConfig) -> str:
    s = config.schema
    rows = ",\n".join(
        f"    ('{table}', '{CONVENTION_VERSION}', '{json.dumps(cfg, sort_keys=True)}'::jsonb)"
        for table, cfg in table_configs(config).items()
    )
    return f"""\
INSERT INTO {s}.store_tables (table_name, convention_version, config)
VALUES
{rows}
ON CONFLICT (table_name) DO NOTHING"""


def generate_ddl(config: StoreConfig) -> list[str]:
    """All provisioning statements, in execution order. Plain Postgres 14+."""
    s = config.schema
    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        _series_table(s),
        _store_tables_table(s),
        _get_series_id_fn(s),
        _register_series_fn(s),
        _runs_table(s),
    ]
    for inst in _instances(config):
        stmts.append(_points_table(s, inst))
        if inst["shape"] == "forecast":
            stmts.append(_asof_index(s, inst["name"]))
            stmts.append(_latest_view(s, inst))
    stmts += _evaluation_tables(s)
    stmts.append(_belief_guard_fn(s))
    for inst in _instances(config):
        if inst["shape"] == "actuals" and not inst["revisions"]:
            stmts.append(_belief_guard_trigger(s, inst["name"]))
    if config.append_only_guard:
        stmts.append(_append_only_guard_fn(s))
        for inst in _instances(config):
            if inst["shape"] != "actuals" or inst["revisions"]:
                stmts.append(_append_only_guard_trigger(s, inst["name"]))
    stmts.append(_seed_store_tables(config))
    return stmts


def hypertable_ddl(config: StoreConfig) -> list[str]:
    """TimescaleDB enhancements; skipped on plain Postgres.

    Columnstore per pg-aiguide's hypertable guidance: segmentby the primary
    filter (series_id — high row density per chunk), orderby forming a natural
    progression within each segment; orderby columns get minmax sparse indexes
    automatically, which are exactly the as-of predicates. Deliberately
    convert-after (create_hypertable with migrate_data) rather than CREATE
    TABLE WITH (tsdb.*): one canonical table DDL everywhere, and re-running
    provision upgrades a populated plain-Postgres store in place.
    """
    s = config.schema
    statements = []
    for inst in _instances(config):
        statements += [
            f"SELECT create_hypertable('{s}.{inst['name']}', 'target_time', "
            "if_not_exists => TRUE, migrate_data => TRUE)",
            f"ALTER TABLE {s}.{inst['name']} SET (timescaledb.enable_columnstore, "
            f"timescaledb.segmentby = 'series_id', timescaledb.orderby = '{inst['orderby']}')",
            f"CALL add_columnstore_policy('{s}.{inst['name']}', after => INTERVAL '7 days', "
            "if_not_exists => true)",
        ]
    # time_bucket-based, so it lives here: a SQL function body is validated at
    # CREATE, and this one must not fail plain-Postgres provisioning.
    statements.append(_sweep_fn(config))
    return statements
