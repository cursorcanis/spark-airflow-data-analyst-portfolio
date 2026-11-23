from atl_transit.warehouse_sql import full_refresh_sql, run_in_transaction, window_refresh_sql


def test_full_refresh_truncates_instead_of_dropping_target():
    sql = full_refresh_sql("raw.gtfs_stop_times", "raw._load_gtfs_stop_times")
    assert "TRUNCATE raw.gtfs_stop_times" in sql
    assert not any(s.startswith("DROP TABLE raw.gtfs_stop_times") for s in sql)   # dbt views depend on it
    assert any("(trip_id, stop_sequence)" in s for s in sql)
    assert sql[-2] == "DROP TABLE raw._load_gtfs_stop_times"


def test_window_refresh_deletes_only_the_window():
    sql = window_refresh_sql("raw.rt_stop_arrivals", "raw._load_rt_stop_arrivals", "service_date")
    assert "DELETE FROM raw.rt_stop_arrivals WHERE service_date >= %(cutoff)s" in sql


class FakeCursor:
    def __init__(self, log, fail_on=None):
        self.log, self.fail_on = log, fail_on

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        if self.fail_on and self.fail_on in stmt:
            raise RuntimeError("boom")
        self.log.append((stmt, params))


class FakeConn:
    def __init__(self, fail_on=None):
        self.log, self.fail_on, self.committed, self.rolled_back = [], fail_on, False, False

    def cursor(self):
        return FakeCursor(self.log, self.fail_on)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_run_in_transaction_binds_params_only_where_used():
    conn = FakeConn()
    run_in_transaction(conn, window_refresh_sql("raw.t", "raw._load_t", "d"), {"cutoff": "2026-11-20"})
    assert conn.committed
    params = {stmt: p for stmt, p in conn.log}
    assert params["DELETE FROM raw.t WHERE d >= %(cutoff)s"] == {"cutoff": "2026-11-20"}
    assert params["INSERT INTO raw.t SELECT * FROM raw._load_t"] is None   # no placeholder -> no params


def test_run_in_transaction_rolls_back_on_error():
    conn = FakeConn(fail_on="INSERT")
    try:
        run_in_transaction(conn, full_refresh_sql("raw.t", "raw._load_t"))
    except RuntimeError:
        pass
    assert conn.rolled_back and not conn.committed
