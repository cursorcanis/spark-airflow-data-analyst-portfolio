"""Curate compacted GTFS-realtime hours into Delta tables with PySpark (via Spark Connect).

Triggered by the RAW_RT_PARQUET asset (emitted when gtfs_rt_hourly finishes an hour).
Incremental processing with a checkpoint: every hour that has a manifest with data but is not in
the state file (s3://curated/_state/rt_curated_hours.json) is processed, oldest first, so missed
triggers, retries and backfills all converge. Writes are Delta MERGEs, so re-processing is safe.

    s3://curated/vehicle_positions   one row per (vehicle, GPS fix), partitioned by event_date
    s3://curated/stop_arrivals       observed vs scheduled arrival per (service_date, trip, stop_sequence)
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from lake_assets import CURATED_STOP_ARRIVALS, CURATED_VEHICLE_POSITIONS, RAW_RT_PARQUET

from atl_transit.gtfs_rt import MANIFEST_PREFIX
from atl_transit.spark_jobs import curate_rt_hour
from atl_transit.spark_session import get_spark

LAKE_CONN_ID = "lake_s3"
STATE_BUCKET, STATE_KEY = "curated", "_state/rt_curated_hours.json"
MAX_HOURS_PER_RUN = 24


def _load_state(hook: S3Hook) -> dict:
    if hook.check_for_key(STATE_KEY, STATE_BUCKET):
        return json.loads(hook.read_key(STATE_KEY, STATE_BUCKET))
    return {"hours": {}}


@dag(
    dag_id="spark_curate_rt",
    schedule=[RAW_RT_PARQUET],
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # Delta MERGEs into the same tables must not run concurrently
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["spark", "curated", "gtfs-rt"],
    doc_md=__doc__,
)
def spark_curate_rt():
    @task
    def pending_hours() -> list[str]:
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        if not hook.list_keys("curated", prefix="gtfs_static/stop_times/_delta_log/"):
            raise RuntimeError("curated GTFS static missing: run spark_curate_gtfs_static first")
        done = _load_state(hook)["hours"]
        pending = []
        for key in sorted(hook.list_keys("raw", prefix=MANIFEST_PREFIX) or []):
            hour_id = key.removeprefix(MANIFEST_PREFIX).removesuffix(".json")  # date=YYYY-MM-DD/hour=HH
            if hour_id in done:
                continue
            manifest = json.loads(hook.read_key(key, "raw"))
            if all(f["rows"] > 0 for f in manifest["feeds"]):
                pending.append(hour_id)
        print(f"{len(pending)} hours pending; processing up to {MAX_HOURS_PER_RUN}")
        return pending[:MAX_HOURS_PER_RUN]

    @task(outlets=[CURATED_VEHICLE_POSITIONS, CURATED_STOP_ARRIVALS])
    def curate(hours: list[str]) -> list[dict]:
        if not hours:
            return []
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        spark = get_spark()
        results = []
        for hour_id in hours:
            date, hour = (p.split("=")[1] for p in hour_id.split("/"))
            stats = curate_rt_hour(spark, "s3a://raw", "s3a://curated", date, hour)
            print(stats)
            results.append(stats)
            # checkpoint after every hour: a failure later in the loop doesn't redo finished hours
            state = _load_state(hook)
            state["hours"][hour_id] = stats
            hook.load_string(json.dumps(state, indent=1), STATE_KEY, STATE_BUCKET, replace=True)
        return results

    curate(pending_hours())


spark_curate_rt()
