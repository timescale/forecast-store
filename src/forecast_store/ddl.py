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
    """The §8 orphan/grid sweep — catalog-driven, so it never regenerates.

    Monitor-first enforcement's backstop: for every points instance *declared
    in* ``store_tables`` (and actually present — dropped tables are skipped)
    it reports series ids absent from the registry (recent write window),
    off-grid ``target_time`` (``time_bucket`` against the registry's declared
    ``sample_interval``; intervals of a month or longer are skipped — no fixed
    stride), and — where the declaration says so — ``target_time_observed``
    outside its target's bucket.

    Iterating the catalog instead of baking table names in means an instance
    added later (with the library or by hand, as long as it declares itself)
    is swept from the moment its ``store_tables`` row exists — a statically
    generated sweep would silently not monitor it. Executes ``time_bucket``,
    so it ships with the TimescaleDB layer (``hypertable_ddl``); scheduling is
    deployment-owned (cron, ``add_job``).
    """
    s = config.schema
    return f"""\
CREATE OR REPLACE FUNCTION {s}.data_quality_sweep(scan_window interval DEFAULT '2 days')
RETURNS TABLE(issue text, table_name text, series_id bigint, n bigint)
LANGUAGE plpgsql STABLE AS $fn$
DECLARE
    t record;
BEGIN
    FOR t IN
        SELECT st.table_name AS name,
               coalesce((st.config->>'has_target_time_observed')::boolean, false)
                   AS observed
        FROM {s}.store_tables st
        WHERE st.config->>'role' IN ('actuals', 'predictors', 'own_forecasts')
          AND to_regclass(format('{s}.%I', st.table_name)) IS NOT NULL
        ORDER BY st.table_name
    LOOP
        RETURN QUERY EXECUTE format($q$
            SELECT 'orphan_series'::text, %L::text, p.series_id, count(*)
            FROM {s}.%I p LEFT JOIN {s}.series sr USING (series_id)
            WHERE p.recorded_at > now() - $1 AND sr.series_id IS NULL
            GROUP BY p.series_id$q$, t.name, t.name) USING scan_window;
        RETURN QUERY EXECUTE format($q$
            SELECT 'off_grid_target_time'::text, %L::text, p.series_id, count(*)
            FROM {s}.%I p JOIN {s}.series sr USING (series_id)
            WHERE p.recorded_at > now() - $1
              AND sr.sample_interval > interval '0'
              AND sr.sample_interval < interval '28 days'
              AND time_bucket(sr.sample_interval, p.target_time) <> p.target_time
            GROUP BY p.series_id$q$, t.name, t.name) USING scan_window;
        IF t.observed THEN
            RETURN QUERY EXECUTE format($q$
                SELECT 'observed_outside_bucket'::text, %L::text, p.series_id, count(*)
                FROM {s}.%I p JOIN {s}.series sr USING (series_id)
                WHERE p.recorded_at > now() - $1
                  AND p.target_time_observed IS NOT NULL
                  AND (p.target_time_observed < p.target_time
                       OR p.target_time_observed >= p.target_time + sr.sample_interval)
                GROUP BY p.series_id$q$, t.name, t.name) USING scan_window;
        END IF;
    END LOOP;
END
$fn$"""


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
    configs.update(_evaluation_configs())
    return configs


def _evaluation_configs() -> dict[str, dict[str, object]]:
    return {
        "evaluation_runs": {"role": "evaluation"},
        "evaluation_series": {"role": "evaluation"},
        "evaluation_metrics": {"role": "evaluation"},
    }


def _seed_rows(schema: str, configs: dict[str, dict[str, object]]) -> str:
    rows = ",\n".join(
        f"    ('{table}', '{CONVENTION_VERSION}', '{json.dumps(cfg, sort_keys=True)}'::jsonb)"
        for table, cfg in configs.items()
    )
    return f"""\
INSERT INTO {schema}.store_tables (table_name, convention_version, config)
VALUES
{rows}
ON CONFLICT (table_name) DO NOTHING"""


def catalog_ddl(config: StoreConfig) -> list[str]:
    """The decision-invariant layer, in execution order. Plain Postgres 14+.

    Registry, self-description catalog, resolvers, run provenance, evaluation
    tables, and the guard *functions* (trigger attachment is a per-table
    decision and lives in :func:`points_ddl`). Nothing here changes when the
    steps-2-to-5 design decisions (spec §6-§7) change: bands, PK switches, and
    extra instances only ever touch the points layer.
    """
    s = config.schema
    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        _series_table(s),
        _store_tables_table(s),
        _get_series_id_fn(s),
        _register_series_fn(s),
        _runs_table(s),
        *_evaluation_tables(s),
        _belief_guard_fn(s),
    ]
    if config.append_only_guard:
        stmts.append(_append_only_guard_fn(s))
    stmts.append(_seed_rows(s, _evaluation_configs()))
    return stmts


def points_ddl(config: StoreConfig) -> list[str]:
    """The decision-derived layer: one self-contained block per instance.

    Each block is a points table with everything it owns — index, serving
    view, guard triggers, and its own ``store_tables`` declaration row — so an
    instance can be added to a provisioned store by executing its block alone
    (the catalog, including the catalog-driven sweep, picks it up from the
    declaration).
    """
    s = config.schema
    declarations = table_configs(config)
    stmts: list[str] = []
    for inst in _instances(config):
        name = inst["name"]
        stmts.append(_points_table(s, inst))
        if inst["shape"] == "forecast":
            stmts.append(_asof_index(s, name))
            stmts.append(_latest_view(s, inst))
        if inst["shape"] == "actuals" and not inst["revisions"]:
            stmts.append(_belief_guard_trigger(s, name))
        if config.append_only_guard and (inst["shape"] != "actuals" or inst["revisions"]):
            stmts.append(_append_only_guard_trigger(s, name))
        stmts.append(_seed_rows(s, {name: declarations[name]}))
    return stmts


def generate_ddl(config: StoreConfig) -> list[str]:
    """All provisioning statements, in execution order (catalog first).
    Plain Postgres 14+."""
    return catalog_ddl(config) + points_ddl(config)


def catalog_hypertable_ddl(config: StoreConfig) -> list[str]:
    """The TimescaleDB half of the catalog layer: the catalog-driven sweep.

    Lives here rather than in :func:`catalog_ddl` because executing it needs
    ``time_bucket`` — on plain Postgres it would fail at first call.
    """
    return [_sweep_fn(config)]


def points_hypertable_ddl(config: StoreConfig) -> list[str]:
    """Per-instance TimescaleDB enhancements; skipped on plain Postgres.

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
    return statements


def hypertable_ddl(config: StoreConfig) -> list[str]:
    """TimescaleDB enhancements; skipped on plain Postgres."""
    return points_hypertable_ddl(config) + catalog_hypertable_ddl(config)
