"""Load curated Delta tables into the Postgres warehouse (raw schema), then let dbt take over.

Three DAGs:
  warehouse_load_gtfs_static  asset CURATED_GTFS_STATIC   -> full refresh of raw.gtfs_*
  warehouse_load_rt           asset CURATED_STOP_ARRIVALS -> lookback-window refresh of raw.rt_stop_arrivals
  warehouse_load_rt_gps       daily 04:30 Atlanta time    -> raw.rt_vehicle_pings + raw.rt_arrival_gps_check
Step 1 (Spark, via Spark Connect): executors write raw._load_<table> over JDBC.
Step 2 (Postgres): publish load -> target in ONE transaction (atomic for readers; see warehouse_sql.py).
The GPS DAG is time-based, not asset-based: it re-reads days of GPS fixes, which is too heavy to repeat every
hour, and 04:30 is MARTA's quietest time. Its asset event still triggers dbt like the others.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from lake_assets import (
    CURATED_GTFS_STATIC,
    CURATED_STOP_ARRIVALS,
    WAREHOUSE_GTFS_STATIC,
    WAREHOUSE_RT_ARRIVALS,
    WAREHOUSE_RT_GPS,
)

from atl_transit.spark_jobs import (
    WAREHOUSE_STATIC_TABLES,
    export_gtfs_static,
    export_rt_gps_window,
    export_stop_arrivals_window,
)
from atl_transit.spark_session import get_spark
from atl_transit.warehouse_sql import full_refresh_sql, run_in_transaction, window_refresh_sql

WAREHOUSE_CONN_ID = "warehouse_pg"
LOOKBACK_DAYS = int(os.environ.get("WAREHOUSE_RT_LOOKBACK_DAYS", "3"))
GPS_LOOKBACK_DAYS = int(os.environ.get("WAREHOUSE_GPS_LOOKBACK_DAYS", "2"))
DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=5)}


def _jdbc() -> dict:
    c = PostgresHook.get_connection(WAREHOUSE_CONN_ID)
    return {"url": f"jdbc:postgresql://{c.host}:{c.port or 5432}/{c.schema}", "user": c.login, "password": c.password}


def _publish(statements: list[str], params: dict | None = None) -> None:
    conn = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()
    try:
        run_in_transaction(conn, statements, params)
    finally:
        conn.close()


@dag(
    dag_id="warehouse_load_gtfs_static",
    schedule=[CURATED_GTFS_STATIC],
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["warehouse", "spark", "gtfs"],
    doc_md=__doc__,
)
def warehouse_load_gtfs_static():
    @task
    def spark_export() -> dict:
        return export_gtfs_static(get_spark(), "s3a://curated", _jdbc())

    @task(outlets=[WAREHOUSE_GTFS_STATIC])
    def publish(counts: dict) -> dict:
        for table in WAREHOUSE_STATIC_TABLES:
            _publish(full_refresh_sql(f"raw.gtfs_{table}", f"raw._load_gtfs_{table}"))
        return counts

    publish(spark_export())


@dag(
    dag_id="warehouse_load_rt",
    schedule=[CURATED_STOP_ARRIVALS],
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["warehouse", "spark", "gtfs-rt"],
    doc_md=__doc__,
)
def warehouse_load_rt():
    @task
    def spark_export() -> dict:
        # Lookback window: recent service dates can still change (late MERGE updates), so re-export them.
        cutoff = (datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
        rows = export_stop_arrivals_window(get_spark(), "s3a://curated", _jdbc(), cutoff)
        return {"cutoff": cutoff, "rows": rows}

    @task(outlets=[WAREHOUSE_RT_ARRIVALS])
    def publish(meta: dict) -> dict:
        _publish(window_refresh_sql("raw.rt_stop_arrivals", "raw._load_rt_stop_arrivals", "service_date"),
                 {"cutoff": meta["cutoff"]})
        return meta

    publish(spark_export())


@dag(
    dag_id="warehouse_load_rt_gps",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["warehouse", "spark", "gtfs-rt", "geospatial"],
    doc_md=__doc__,
)
def warehouse_load_rt_gps():
    @task
    def spark_export() -> dict:
        cutoff = (datetime.now(UTC) - timedelta(days=GPS_LOOKBACK_DAYS)).date().isoformat()
        counts = export_rt_gps_window(get_spark(), "s3a://curated", _jdbc(), cutoff)
        return {"cutoff": cutoff, **counts}

    @task(outlets=[WAREHOUSE_RT_GPS])
    def publish(meta: dict) -> dict:
        # Both tables in ONE transaction: pings and the GPS check always describe the same days.
        _publish(window_refresh_sql("raw.rt_vehicle_pings", "raw._load_rt_vehicle_pings", "event_date")
                 + window_refresh_sql("raw.rt_arrival_gps_check", "raw._load_rt_arrival_gps_check", "service_date"),
                 {"cutoff": meta["cutoff"]})
        return meta

    publish(spark_export())


warehouse_load_gtfs_static()
warehouse_load_rt()
warehouse_load_rt_gps()
