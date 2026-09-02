"""CLI: the skill's executable surface.

    forecast-store ddl       [--config FILE | --band 0.05,... --no-mean --single-belief-actuals]
                             [--schema S] [--section all|catalog|tables] [--timescale]
    forecast-store provision [--config FILE | trio flags] [--schema S] [--dsn DSN] [--no-timescale]
    forecast-store register-series NAME --interval INTERVAL [--timezone TZ] [--unit U]
                             [--description TEXT] [--metadata JSON] [--dsn DSN] [--schema S]
    forecast-store describe  [--dsn DSN] [--schema S] [--config FILE]

``--config`` is a YAML declaration (:mod:`forecast_store.declaration`) — any
set of tables. The trio flags are shortcuts for the conventional three
(``forecasts`` / ``predictors`` / ``actuals``) and cannot be combined with it;
``--schema`` may override either. ``--dsn`` defaults to ``$FORECAST_STORE_DSN``.

``describe`` prints the store's declaration as a YAML file you can edit and
pass back to ``provision --config``; with ``--config`` it is a drift check
instead — per table: differs / missing (provisioning would add it) /
unmanaged (in the store, not the file; left untouched) — and exits 1 on
drift. Exit codes: 0 ok, 1 drift or database error, 2 usage.

``--section catalog`` prints the decision-invariant layer (registry, catalog,
resolvers, evaluation, guard functions, sweep); ``--section tables`` prints
the points-table blocks the declaration holds.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys

from forecast_store.config import LIANDER_BAND, StoreConfig
from forecast_store.ddl import (
    catalog_ddl,
    catalog_hypertable_ddl,
    generate_ddl,
    hypertable_ddl,
    points_ddl,
    points_hypertable_ddl,
)
from forecast_store.errors import ForecastStoreError, InvalidDeclaration, NotProvisioned


def _add_declaration_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--config", metavar="FILE",
        help="YAML declaration — any set of tables; excludes the trio flags",
    )
    p.add_argument(
        "--band",
        help="quantile levels of the conventional trio, comma-separated (default: liander band)",
    )
    p.add_argument("--no-mean", action="store_true", help="trio: omit the mean column")
    p.add_argument(
        "--single-belief-actuals", action="store_true",
        help="trio: actuals admit one belief per target (default: revisioned)",
    )
    p.add_argument("--schema", help="store schema (default: forecast, or the file's)")


def _declaration(parser: argparse.ArgumentParser, args: argparse.Namespace) -> StoreConfig:
    trio = [
        flag for flag, on in (
            ("--band", args.band),
            ("--no-mean", args.no_mean),
            ("--single-belief-actuals", args.single_belief_actuals),
        ) if on
    ]
    if args.config:
        if trio:
            parser.error(f"--config declares the tables; drop {', '.join(trio)}")
        from forecast_store.declaration import load

        try:
            config = load(args.config)
        except (InvalidDeclaration, OSError) as exc:
            parser.error(f"--config {args.config}: {exc}")
        return dataclasses.replace(config, schema=args.schema) if args.schema else config
    return StoreConfig.standard(
        args.band.split(",") if args.band else LIANDER_BAND,
        has_mean=not args.no_mean,
        actuals_revisions=not args.single_belief_actuals,
        schema=args.schema or "forecast",
    )


def _dsn(parser: argparse.ArgumentParser, args: argparse.Namespace) -> str:
    dsn = args.dsn or os.environ.get("FORECAST_STORE_DSN")
    if not dsn:
        parser.error("--dsn or FORECAST_STORE_DSN required")
    return dsn


def _register_series(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    import psycopg

    from forecast_store.store import Store

    dsn = _dsn(parser, args)
    metadata = None
    if args.metadata:
        try:
            metadata = json.loads(args.metadata)
        except json.JSONDecodeError as exc:
            parser.error(f"--metadata must be a JSON object: {exc}")
        if not isinstance(metadata, dict):
            parser.error("--metadata must be a JSON object")
    try:
        with Store.connect(dsn, schema=args.schema) as store:
            series_id = store.register_series(
                args.name, args.interval,
                timezone=args.timezone, unit=args.unit,
                description=args.description, metadata=metadata,
            )
    except NotProvisioned as exc:
        parser.error(str(exc))
    except (ForecastStoreError, psycopg.Error) as exc:
        print(f"register-series: {str(exc).splitlines()[0]}", file=sys.stderr)
        return 1
    print(series_id)
    return 0


def _describe(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    import psycopg

    from forecast_store.declaration import dumps, load
    from forecast_store.provision import compare_declarations, stored_declarations

    dsn = _dsn(parser, args)
    schema = args.schema
    with psycopg.connect(dsn) as conn:
        try:
            loaded = StoreConfig.from_store(conn, schema)
            rows = stored_declarations(conn, schema)
        except (NotProvisioned, InvalidDeclaration) as exc:
            parser.error(str(exc))
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT string_agg(DISTINCT convention_version, ', ') FROM {schema}.store_tables"
            )
            versions = cur.fetchone()[0]
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
            engine = "timescaledb" if cur.fetchone() is not None else "plain postgres"
            cur.execute(f"SELECT count(*) FROM {schema}.series")
            n_series = cur.fetchone()[0]
    header = f"# store '{schema}': convention {versions}, {engine}, {n_series} series\n"

    if not args.config:
        print(header + dumps(loaded), end="")
        return 0

    try:
        requested = load(args.config)
    except (InvalidDeclaration, OSError) as exc:
        parser.error(f"--config {args.config}: {exc}")
    drift = compare_declarations(rows, requested)
    lines = [
        f"differs    {table}\n  store: {stored}\n  file:  {wanted}"
        for table, (stored, wanted) in drift.differs.items()
    ]
    lines += [f"missing    {t}  (provision would add it)" for t in drift.missing]
    lines += [
        f"unmanaged  {t}  (in the store, not in the file; left untouched)"
        for t in drift.unmanaged
    ]
    store_level = []
    if requested.schema != schema:
        store_level.append(f"schema: store '{schema}', file '{requested.schema}'")
    if requested.append_only_guard != loaded.append_only_guard:
        store_level.append(
            f"append_only_guard: store {loaded.append_only_guard}, "
            f"file {requested.append_only_guard}"
        )
    drifted = bool(drift) or bool(store_level)
    print(header, end="")
    for line in lines + store_level:
        print(line)
    print("DRIFT: the store is not what the file declares" if drifted else "in sync")
    return 1 if drifted else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="forecast-store")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ddl = sub.add_parser("ddl", help="print provisioning DDL")
    _add_declaration_args(p_ddl)
    p_ddl.add_argument("--timescale", action="store_true", help="include hypertable statements")
    p_ddl.add_argument(
        "--section", choices=("all", "catalog", "tables"), default="all",
        help="catalog = decision-invariant layer; tables = points-table blocks",
    )

    p_prov = sub.add_parser(
        "provision", help="create or verify a store (new tables are added; drift is refused)"
    )
    _add_declaration_args(p_prov)
    p_prov.add_argument("--dsn", help="connection string (default: $FORECAST_STORE_DSN)")
    p_prov.add_argument("--no-timescale", action="store_true")

    p_reg = sub.add_parser(
        "register-series", help="register a series (get-or-create) and print its id"
    )
    p_reg.add_argument("name")
    p_reg.add_argument(
        "--interval", required=True, help="sample interval, e.g. '15 minutes' or 'PT15M'"
    )
    p_reg.add_argument("--timezone")
    p_reg.add_argument("--unit")
    p_reg.add_argument("--description")
    p_reg.add_argument("--metadata", help="JSON object")
    p_reg.add_argument("--dsn", help="connection string (default: $FORECAST_STORE_DSN)")
    p_reg.add_argument("--schema", default="forecast")

    p_desc = sub.add_parser(
        "describe", help="print a store's declaration as YAML, or check a file against it"
    )
    p_desc.add_argument("--dsn", help="connection string (default: $FORECAST_STORE_DSN)")
    p_desc.add_argument("--schema", default="forecast")
    p_desc.add_argument(
        "--config", metavar="FILE", help="declaration to check against the store; exit 1 on drift"
    )

    args = parser.parse_args(argv)

    if args.command == "ddl":
        config = _declaration(p_ddl, args)
        if args.section == "catalog":
            statements = catalog_ddl(config)
            if args.timescale:
                statements += catalog_hypertable_ddl(config)
        elif args.section == "tables":
            statements = points_ddl(config)
            if args.timescale:
                statements += points_hypertable_ddl(config)
        else:
            statements = generate_ddl(config)
            if args.timescale:
                statements += hypertable_ddl(config)
        print(";\n\n".join(statements) + ";")
        return 0

    if args.command == "provision":
        config = _declaration(p_prov, args)
        dsn = _dsn(p_prov, args)
        from forecast_store.provision import provision

        report = provision(dsn, config, timescale=False if args.no_timescale else None)
        mode = "timescaledb" if report.timescaledb else "plain postgres"
        state = "verified existing" if report.already_provisioned else "created"
        print(
            f"{state} store '{report.schema}' ({mode}), "
            f"{report.statements_executed} statements executed"
        )
        return 0

    if args.command == "register-series":
        return _register_series(p_reg, args)

    if args.command == "describe":
        return _describe(p_desc, args)

    return 2  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
