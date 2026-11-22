"""Daily retention for raw GTFS-realtime snapshots.

Raw protobuf snapshots are ~150-300 MB/day. Once an hour has been compacted to Parquet (its
manifest exists), snapshots older than RAW_RT_RETENTION_DAYS are deleted. Hours that were never
compacted are kept, so nothing is lost if the hourly DAG fell behind.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

from atl_transit.gtfs_rt import FEEDS, MANIFEST_PREFIX, SNAPSHOT_PREFIX, purgeable_keys

LAKE_CONN_ID = "lake_s3"
RAW_BUCKET = os.environ.get("LAKE_RAW_BUCKET", "raw")
RETENTION_DAYS = int(os.environ.get("RAW_RT_RETENTION_DAYS", "14"))


@dag(
    dag_id="lake_housekeeping",
    schedule="30 3 * * *",  # 03:30 Atlanta time, low activity
    start_date=pendulum.datetime(2026, 11, 22, tz="America/New_York"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15)},
    tags=["maintenance", "lake"],
    doc_md=__doc__,
)
def lake_housekeeping():
    @task
    def purge_old_snapshots() -> dict:
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)
        compacted = {
            k.removeprefix(MANIFEST_PREFIX).removesuffix(".json")
            for k in hook.list_keys(RAW_BUCKET, prefix=MANIFEST_PREFIX) or []
        }
        stats = {"deleted": 0, "kept_uncompacted": 0, "cutoff": f"{cutoff:%Y-%m-%d %H:00}"}
        for feed in FEEDS:
            keys = hook.list_keys(RAW_BUCKET, prefix=f"{SNAPSHOT_PREFIX}/feed={feed}/") or []
            delete, kept = purgeable_keys(keys, compacted, cutoff)
            for i in range(0, len(delete), 1000):  # S3 DeleteObjects takes max 1000 keys per call
                hook.delete_objects(RAW_BUCKET, delete[i : i + 1000])
            stats["deleted"] += len(delete)
            stats["kept_uncompacted"] += kept
        print(stats)
        return stats

    purge_old_snapshots()


lake_housekeeping()
