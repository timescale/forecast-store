"""CLI: the skill's executable surface.

    forecast-store ddl       [--config FILE | --band 0.05,... --no-mean --single-belief-actuals]
                             [--schema S] [--section all|catalog|tables] [--timescale]
    forecast-store provision [--config FILE | trio flags] [--schema S] [--dsn DSN] [--no-timescale]

``--config`` is a YAML declaration (:mod:`forecast_store.declaration`) — any
set of tables. The trio flags are shortcuts for the conventional three
(``forecasts`` / ``predictors`` / ``actuals``) and cannot be combined with it;
``--schema`` may override either. ``--dsn`` defaults to ``$FORECAST_STORE_DSN``.

``--section catalog`` prints the decision-invariant layer (registry, catalog,
resolvers, evaluation, guard functions, sweep); ``--section tables`` prints
the points-table blocks the declaration holds.
"""

from __future__ import annotations

import argparse
import dataclasses
import os

from forecast_store.config import LIANDER_BAND, StoreConfig
from forecast_store.ddl import (
    catalog_ddl,
    catalog_hypertable_ddl,
    generate_ddl,
    hypertable_ddl,
    points_ddl,
    points_hypertable_ddl,
)
from forecast_store.errors import InvalidDeclaration


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

    return 2  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
