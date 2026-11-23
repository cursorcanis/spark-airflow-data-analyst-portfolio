"""SQL that publishes Spark-written load tables into the warehouse's raw schema, atomically.

Two patterns:
  * full refresh (GTFS static):        TRUNCATE target; INSERT all rows from the load table
  * lookback-window refresh (realtime): DELETE target rows in the window; INSERT the window
Both run inside one transaction, so readers see the old data or the new data, never a mix.
TRUNCATE (not DROP) keeps the table, so dbt views built on it stay valid.
"""

from __future__ import annotations

INDEXES = {
    "raw.gtfs_stop_times": ["(trip_id, stop_sequence)"],
    "raw.gtfs_trips": ["(trip_id)"],
    "raw.gtfs_stops": ["(stop_id)"],
    "raw.rt_stop_arrivals": ["(service_date)", "(trip_id, stop_sequence)"],
}


def _index_sql(table: str) -> list[str]:
    name = table.split(".")[1]
    return [
        f"CREATE INDEX IF NOT EXISTS {name}_{i}_idx ON {table} {cols}"
        for i, cols in enumerate(INDEXES.get(table, []))
    ]


def full_refresh_sql(target: str, load: str) -> list[str]:
    return [
        f"CREATE TABLE IF NOT EXISTS {target} (LIKE {load})",
        f"TRUNCATE {target}",
        f"INSERT INTO {target} SELECT * FROM {load}",
        *_index_sql(target),
        f"DROP TABLE {load}",
        f"ANALYZE {target}",
    ]


def window_refresh_sql(target: str, load: str, date_column: str) -> list[str]:
    """Expects one bound parameter: the cutoff date (%(cutoff)s)."""
    return [
        f"CREATE TABLE IF NOT EXISTS {target} (LIKE {load})",
        f"DELETE FROM {target} WHERE {date_column} >= %(cutoff)s",
        f"INSERT INTO {target} SELECT * FROM {load}",
        *_index_sql(target),
        f"DROP TABLE {load}",
        f"ANALYZE {target}",
    ]


def run_in_transaction(conn, statements: list[str], params: dict | None = None) -> None:
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt, params if "%(" in stmt else None)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
