"""Ingest one liander2024 benchmark target into a forecast store.

Downloads the target's parquets from HuggingFace (cached), registers series
with location/limit metadata per spec §5.1/§10.5, and COPYs the points:
measurements -> revisioned actuals with the dataset's real per-row claims (the
liander feed publishes with a ~48h settlement lag), weather columns -> one
predictor series each, full vintage history.

Idempotent: a series with existing points is skipped.

    uv run --extra openstef --extra foundation python scripts/ingest_liander.py \
        [--group wind_park] [--target "<name>"] [--dsn ...]
"""

from __future__ import annotations

import argparse
import math
import os
import re
from pathlib import Path

import psycopg


def _load_dotenv() -> None:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


_load_dotenv()

# Every covariate column the dataset's weather vintages carry. The OpenSTEF 4
# preset models (xgboost/gblinear) consume the full set; Chronos-2 runs select
# the official example's three-column subset (CHRONOS_FEATURES).
WEATHER_COLUMNS = (
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_speed_80m",
    "wind_direction_10m",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
)
#: The covariates OpenSTEF's own Chronos-2 benchmark example selects.
CHRONOS_FEATURES = ("shortwave_radiation", "wind_speed_80m", "temperature_2m")
REPO = "OpenSTEF/liander2024-stef-benchmark"


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def series_names(group: str, target_name: str) -> tuple[str, dict[str, str]]:
    base = f"ln24/{group}/{slugify(target_name)}"
    return f"{base}/load", {col: f"{base}/wx/{col}" for col in WEATHER_COLUMNS}


def load_group_entries(group: str) -> list[dict]:
    import yaml
    from huggingface_hub import hf_hub_download

    path = hf_hub_download(repo_id=REPO, repo_type="dataset", filename="liander2024_targets.yaml")
    targets = yaml.safe_load(open(path))
    return [t for t in targets if t["group_name"] == group]


def load_target_entry(group: str, target_name: str | None) -> dict:
    group_targets = load_group_entries(group)
    if target_name is None:
        return group_targets[0]
    return next(t for t in group_targets if t["name"] == target_name)


def _register(conn, name: str, metadata: dict) -> int:
    from datetime import timedelta

    from forecast_store.config import StoreConfig
    from forecast_store.series import register_series

    return register_series(
        conn, StoreConfig(), name, timedelta(minutes=15), timezone="UTC", metadata=metadata
    )


def _has_points(cur, table: str, series_id: int) -> bool:
    cur.execute(
        f"SELECT EXISTS (SELECT 1 FROM forecast.{table} WHERE series_id = %s)", (series_id,)
    )
    return cur.fetchone()[0]


def _copy(cur, table: str, columns: str, rows) -> int:
    n = 0
    with cur.copy(f"COPY forecast.{table} ({columns}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
            n += 1
    return n


def ingest_target(conn, group: str, entry: dict) -> None:
    import pandas as pd
    from huggingface_hub import hf_hub_download

    name = entry["name"]
    load_series, weather_series = series_names(group, name)
    metadata = {
        "dataset": "liander2024-stef-benchmark",
        "location": {"latitude": entry["latitude"], "longitude": entry["longitude"]},
        "limits": {"upper": entry.get("upper_limit"), "lower": entry.get("lower_limit")},
        "tags": {"group": group, "target": name},
    }

    with conn.cursor() as cur:
        load_id = _register(conn, load_series, metadata)
        if _has_points(cur, "actuals", load_id):
            print(f"skip  {load_series} (already ingested)")
        else:
            path = hf_hub_download(
                repo_id=REPO, repo_type="dataset",
                filename=f"load_measurements/{group}/{name}.parquet",
            )
            frame = pd.read_parquet(path)
            n = _copy(
                cur,
                "actuals",
                "series_id, target_time, available_at, value",
                (
                    (load_id, row.timestamp, row.available_at, float(row.load))
                    for row in frame.itertuples()
                    if not math.isnan(row.load)
                ),
            )
            print(f"actuals  {load_series}: {n} rows (real per-row claims)")

        wx_path = hf_hub_download(
            repo_id=REPO, repo_type="dataset",
            filename=f"weather_forecasts_versioned/{group}/{name}.parquet",
        )
        weather = pd.read_parquet(wx_path)
        for col, series_name in weather_series.items():
            sid = _register(conn, series_name, metadata)
            if _has_points(cur, "predictors", sid):
                print(f"skip  {series_name} (already ingested)")
                continue
            sub = weather[["timestamp", "available_at", col]].dropna()
            n = _copy(
                cur,
                "predictors",
                "series_id, target_time, available_at, value",
                ((sid, ts, avail, float(v)) for ts, avail, v in sub.itertuples(index=False)),
            )
            print(f"predictors  {series_name}: {n} vintage rows")
    conn.commit()  # one transaction per target: a killed ingest resumes cleanly


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="wind_park")
    parser.add_argument("--target", default=None)
    parser.add_argument("--all-targets", action="store_true", help="ingest every target in the group")
    parser.add_argument("--dsn", default=os.environ.get("FORECAST_STORE_TEST_DSN"))
    args = parser.parse_args()

    entries = (
        load_group_entries(args.group)
        if args.all_targets
        else [load_target_entry(args.group, args.target)]
    )
    with psycopg.connect(args.dsn) as conn:
        for entry in entries:
            print(f"=== {entry['name']} ===")
            ingest_target(conn, args.group, entry)
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
