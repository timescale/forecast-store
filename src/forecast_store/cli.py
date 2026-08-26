"""CLI: the skill's executable surface.

    forecast-store ddl [--band 0.05,0.1,...] [--schema forecast] [--single-belief-actuals] [--no-mean]
    forecast-store provision --dsn postgres://...  [same options] [--no-timescale]
"""

from __future__ import annotations

import argparse
import os
import sys

from forecast_store.config import StoreConfig
from forecast_store.ddl import generate_ddl, hypertable_ddl


def _config_from_args(args: argparse.Namespace) -> StoreConfig:
    kwargs: dict = {
        "schema": args.schema,
        "has_mean": not args.no_mean,
        "actuals_revisions": not args.single_belief_actuals,
    }
    if args.band:
        return StoreConfig.from_levels(args.band.split(","), **kwargs)
    return StoreConfig(**kwargs)


def _add_config_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--band", help="comma-separated quantile levels; default: liander band")
    p.add_argument("--schema", default="forecast")
    p.add_argument(
        "--single-belief-actuals", action="store_true",
        help="actuals admit one belief per target (default: revisioned)",
    )
    p.add_argument("--no-mean", action="store_true", help="omit the mean column")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="forecast-store")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ddl = sub.add_parser("ddl", help="print provisioning DDL")
    _add_config_args(p_ddl)
    p_ddl.add_argument("--timescale", action="store_true", help="include hypertable statements")

    p_prov = sub.add_parser("provision", help="provision a store")
    _add_config_args(p_prov)
    p_prov.add_argument("--dsn", default=os.environ.get("FORECAST_STORE_DSN"))
    p_prov.add_argument("--no-timescale", action="store_true")

    args = parser.parse_args(argv)
    config = _config_from_args(args)

    if args.command == "ddl":
        statements = generate_ddl(config)
        if args.timescale:
            statements += hypertable_ddl(config)
        print(";\n\n".join(statements) + ";")
        return 0

    if args.command == "provision":
        if not args.dsn:
            print("provision: --dsn or FORECAST_STORE_DSN required", file=sys.stderr)
            return 2
        from forecast_store.provision import provision

        report = provision(args.dsn, config, timescale=False if args.no_timescale else None)
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
