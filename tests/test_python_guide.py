"""The Python guide's examples run, in order, against the live store (DX item 14):
an API change fails here instead of silently drifting the docs."""

import os
import re
from pathlib import Path

import pytest

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")
GUIDE = Path(__file__).resolve().parent.parent / "docs" / "python-guide.md"
BLOCK = re.compile(r"^```python\n(.*?)^```", re.S | re.M)
SKIP_MARKER = "# not executed"


def _blocks():
    for match in BLOCK.finditer(GUIDE.read_text()):
        code = match.group(1)
        if not code.lstrip().startswith(SKIP_MARKER):
            yield code


def test_guide_has_examples():
    blocks = list(_blocks())
    assert len(blocks) >= 8, "the guide lost its executable examples"
    skipped = GUIDE.read_text().count(SKIP_MARKER)
    assert skipped >= 2  # the pool and openstef blocks are marked, not silently dropped


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_guide_examples_run_in_order(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    pytest.importorskip("pandas")
    monkeypatch.setenv("FORECAST_STORE_DSN", DSN)  # the guide's first block reads it

    _cleanup(psycopg)
    namespace: dict = {"__name__": "python_guide"}
    try:
        for number, code in enumerate(_blocks(), 1):
            try:
                exec(compile(code, f"python-guide.md block {number}", "exec"), namespace)
            except Exception as exc:  # noqa: BLE001 — report which block, then fail
                pytest.fail(
                    f"guide block {number} failed: {type(exc).__name__}: {exc}\n---\n{code}"
                )
        # The examples produced what the prose says they do.
        assert namespace["history"].gaps == 1  # the row measured *now* is invisible as of t0
        drift = namespace["drift"]
        assert drift.differs == {} and "guide_workspace" in drift.unmanaged
    finally:
        _cleanup(psycopg)


def _cleanup(psycopg):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        cur.execute(
            "DELETE FROM forecast.forecasts WHERE run_id IN "
            "(SELECT run_id FROM forecast.runs WHERE run_name LIKE %s)", ("guide/%",),
        )
        cur.execute("DROP VIEW IF EXISTS forecast.latest_guide_workspace")
        cur.execute("DROP TABLE IF EXISTS forecast.guide_workspace")
        cur.execute("DELETE FROM forecast.store_tables WHERE table_name = 'guide_workspace'")
        cur.execute("DELETE FROM forecast.runs WHERE run_name LIKE %s", ("guide/%",))
        for table in ("actuals", "predictors"):
            cur.execute(
                f"DELETE FROM forecast.{table} WHERE series_id IN "
                "(SELECT series_id FROM forecast.series WHERE name LIKE %s)", ("guide/%",),
            )
        cur.execute("DELETE FROM forecast.series WHERE name LIKE %s", ("guide/%",))
