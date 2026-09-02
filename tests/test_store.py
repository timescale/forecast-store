"""The Store facade (DX item 5): one object bound to a connection and a
declaration, never committing itself. ``Store.connect`` opens a connection
for a block from a DSN or a pool; the block is the unit of work."""

import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from forecast_store import Store, StoreConfig
from forecast_store.store import _connection

DSN = os.environ.get("FORECAST_STORE_TEST_DSN")

T0 = datetime(2024, 10, 1, tzinfo=timezone.utc)
H = timedelta(hours=1)
SERIES = "store_smoke_series"
RUN = "store_smoke"


class _FakeConn:
    def __init__(self):
        self.committed = 0

    def commit(self):
        self.committed += 1


class _FakePool:
    """psycopg_pool's shape: ``.connection()`` is a context manager that
    hands out a connection and commits on normal exit."""

    def __init__(self, conn):
        self.conn, self.checkouts = conn, 0

    @contextmanager
    def connection(self):
        self.checkouts += 1
        yield self.conn
        self.conn.commit()


class _PoolOfRealConnections:
    """A pool stand-in over psycopg (psycopg_pool is not a dependency): one
    checkout = one ``psycopg.connect`` context, which commits on exit."""

    def __init__(self, dsn):
        self.dsn = dsn

    @contextmanager
    def connection(self):
        import psycopg

        with psycopg.connect(self.dsn) as conn:
            yield conn


def test_binding_resolves_schema_and_refuses_contradictions():
    conn = _FakeConn()
    assert Store(conn).schema == "forecast"
    assert Store(conn, schema="fs_a").schema == "fs_a"
    assert Store(conn, StoreConfig(schema="fs_a")).schema == "fs_a"
    assert Store(conn, StoreConfig(schema="fs_a"), schema="fs_a").schema == "fs_a"
    with pytest.raises(ValueError, match="conflicts"):
        Store(conn, StoreConfig(schema="fs_a"), schema="fs_b")
    cfg = StoreConfig()
    store = Store(conn, cfg)
    assert store.config is cfg and store.conn is conn  # explicit config is never re-read


def test_connect_takes_a_pool_by_duck_typing_and_never_commits_itself():
    conn = _FakeConn()
    pool = _FakePool(conn)
    with Store.connect(pool, StoreConfig()) as store:
        assert store.conn is conn
        assert conn.committed == 0  # the facade commits nothing
    assert (pool.checkouts, conn.committed) == (1, 1)  # the pool's exit did


def test_connect_fails_fast_on_what_it_cannot_open():
    with pytest.raises(ValueError, match="conflicts"):  # before any connection attempt
        with Store.connect("postgresql://nowhere/x", StoreConfig(schema="a"), schema="b"):
            pass
    with pytest.raises(TypeError, match="DSN string or a pool"):
        with _connection(42):
            pass


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_store_live():
    """Register, write, read and query through one Store; the block commits
    and a fresh connection sees it; an exception rolls the block back; an
    open connection binds with Store(conn) and is refused by Store.connect."""
    psycopg = pytest.importorskip("psycopg")
    from forecast_store import UnknownSeries, provision

    provision(DSN)
    _cleanup(psycopg)
    try:
        with Store.connect(DSN) as store:  # declaration read from the store
            assert store.config.table("forecasts") == StoreConfig().table("forecasts")
            store.register_series(SERIES, "1 hour")
            store.write_actuals(SERIES, [(T0 - H, 1.0)])
            store.write_predictors(SERIES, [(T0, 2.0)], available_at=T0 - H)
            run_id = store.write_forecast(
                series=SERIES, model="m", run_name=RUN, available_at=T0 - H,
                points=[(T0, {"q50": 3.0})],
            )
            _, rows = store.read_context_series(
                SERIES, table="predictors", start=T0, end=T0 + H, asof=T0
            )
            assert [v for _, _, v in rows] == [2.0]
            asof = store.forecast_asof(SERIES, T0, T0 + H, T0)  # executed, not built
            assert len(asof.rows) == 1
            assert asof.rows[0][asof.columns.index("run_id")] == run_id

        # Leaving the block committed: a fresh connection sees everything.
        with psycopg.connect(DSN) as conn:
            store = Store(conn, StoreConfig())  # caller's connection: caller commits
            assert store.get_series_id(SERIES) > 0
            _, rows = store.read_versioned_series(SERIES, table="actuals", start=T0 - H, end=T0)
            assert [v for *_, v in rows] == [1.0]
            with pytest.raises(TypeError, match=r"Store\(conn"):
                with Store.connect(conn):
                    pass

        # A pool checkout is the unit of work.
        with Store.connect(_PoolOfRealConnections(DSN)) as store:
            store.write_actuals(SERIES, [(T0 + 2 * H, 5.0)])
        with Store.connect(DSN) as store:
            _, rows = store.read_versioned_series(
                SERIES, table="actuals", start=T0 + 2 * H, end=T0 + 3 * H
            )
            assert [v for *_, v in rows] == [5.0]

        # An exception rolls the block back: nothing lands.
        with pytest.raises(RuntimeError, match="abort"):
            with Store.connect(DSN) as store:
                store.write_actuals(SERIES, [(T0 + H, 9.0)])
                raise RuntimeError("abort")
        with Store.connect(DSN) as store:
            _, rows = store.read_versioned_series(
                SERIES, table="actuals", start=T0 + H, end=T0 + 2 * H
            )
            assert rows == []
            with pytest.raises(UnknownSeries):
                store.get_series_id("store_smoke_typo")
    finally:
        _cleanup(psycopg)


@pytest.mark.skipif(not DSN, reason="FORECAST_STORE_TEST_DSN not set")
def test_provision_in_the_callers_transaction():
    """provision(conn, ...) runs in the caller's transaction and commits
    nothing: a rollback leaves no trace of the new instance."""
    psycopg = pytest.importorskip("psycopg")
    from forecast_store import ActualsSpec, provision

    config = StoreConfig().with_tables(ActualsSpec("prov_smoke"))
    with psycopg.connect(DSN) as conn:
        report = provision(conn, config)
        assert report.already_provisioned  # additive onto the shared test store
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('forecast.prov_smoke')")
            assert cur.fetchone()[0] is not None  # visible inside the transaction
        conn.rollback()  # the caller's call
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('forecast.prov_smoke')")
            assert cur.fetchone()[0] is None
            cur.execute("SELECT 1 FROM forecast.store_tables WHERE table_name = 'prov_smoke'")
            assert cur.fetchone() is None


def _cleanup(psycopg):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        cur.execute(
            "DELETE FROM forecast.forecasts WHERE run_id IN "
            "(SELECT run_id FROM forecast.runs WHERE run_name = %s)", (RUN,),
        )
        cur.execute("DELETE FROM forecast.runs WHERE run_name = %s", (RUN,))
        for table in ("actuals", "predictors"):
            cur.execute(
                f"DELETE FROM forecast.{table} WHERE series_id IN "
                "(SELECT series_id FROM forecast.series WHERE name = %s)", (SERIES,),
            )
        cur.execute("DELETE FROM forecast.series WHERE name = %s", (SERIES,))
