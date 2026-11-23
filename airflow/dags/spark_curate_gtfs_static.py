"""Curate the latest GTFS static feed into typed Delta tables (s3://curated/gtfs_static/<table>).

Triggered by the RAW_GTFS_STATIC asset, i.e. only when gtfs_static_weekly actually landed a new
feed version. Full refresh per table; Delta keeps the previous versions (time travel).
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from lake_assets import CURATED_GTFS_STATIC, RAW_GTFS_STATIC

from atl_transit.spark_jobs import curate_gtfs_static
from atl_transit.spark_session import get_spark

LAKE_CONN_ID = "lake_s3"


@dag(
    dag_id="spark_curate_gtfs_static",
    schedule=[RAW_GTFS_STATIC],
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["spark", "curated", "gtfs"],
    doc_md=__doc__,
)
def spark_curate_gtfs_static():
    @task(outlets=[CURATED_GTFS_STATIC])
    def curate() -> dict:
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        if not hook.check_for_bucket("curated"):
            hook.create_bucket("curated")
        latest = json.loads(hook.read_key("gtfs_static/_latest.json", "raw"))
        ingest_date = latest["downloaded_at"][:10]
        tables = f"s3a://raw/gtfs_static/ingest_date={ingest_date}/tables"

        counts = curate_gtfs_static(get_spark(), tables, "s3a://curated", ingest_date)
        print(f"feed {ingest_date}: {counts}")
        if counts.get("stop_times", 0) == 0 or counts.get("trips", 0) == 0:
            raise ValueError(f"curated core tables are empty: {counts}")
        return {"ingest_date": ingest_date, "rows": counts}

    curate()


spark_curate_gtfs_static()
