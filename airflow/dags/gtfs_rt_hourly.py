"""Hourly compaction of MARTA GTFS-realtime snapshots into Parquet.

The `rt-poller` service lands one gzip protobuf snapshot per feed refresh:
    s3://raw/gtfs_rt/feed=<feed>/date=YYYY-MM-DD/hour=HH/<feed>_<unix_ts>.pb.gz
Each hourly run (data interval = that UTC hour) does, per feed (dynamic task mapping):
    wait 5 min for late files -> decode every snapshot -> flatten to rows -> one Parquet file
    s3://raw/gtfs_rt_parquet/feed=<feed>/date=YYYY-MM-DD/hour=HH/part-000.parquet
then checks completeness (snapshot count vs. expected) and writes an hourly manifest.
catchup=True: if the server was off, missed hours are compacted automatically when it returns.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import timedelta

import pendulum
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from lake_assets import RAW_RT_PARQUET

from atl_transit.gtfs_rt import (
    FLATTENERS,
    SCHEMAS,
    feed_ts_from_key,
    hour_partition,
    manifest_key,
    parquet_key,
    parse_feed,
    snapshot_prefix,
    to_table,
)

LAKE_CONN_ID = "lake_s3"
RAW_BUCKET = os.environ.get("LAKE_RAW_BUCKET", "raw")
# Expected snapshots per hour = 3600 / poll interval; below this share we flag a capture gap.
EXPECTED_INTERVAL_S = {
    "vehicle_positions": int(os.environ.get("POLL_VEHICLE_POSITIONS_S", "30")),
    "trip_updates": int(os.environ.get("POLL_TRIP_UPDATES_S", "120")),
}
MIN_COMPLETENESS = 0.8


@dag(
    dag_id="gtfs_rt_hourly",
    # Airflow 3's plain "@hourly" (CronTriggerTimetable) fires AT the hour with an empty data interval.
    # We need "process the hour that just ended", i.e. a data interval [H, H+1) that runs after H+1:
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 22, 22, tz="UTC"),  # first hour with captured data
    catchup=True,
    max_active_runs=2,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingest", "gtfs-rt", "marta", "compaction"],
    doc_md=__doc__,
)
def gtfs_rt_hourly():
    # The run for hour H starts at H+1:00; give the poller 5 minutes to land the last snapshots.
    wait_for_late_files = TimeDeltaSensor(
        task_id="wait_for_late_files", delta=timedelta(minutes=5), mode="reschedule", poke_interval=60
    )

    @task
    def compact(feed: str, data_interval_start=None) -> dict:
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        hour = data_interval_start
        keys = sorted(hook.list_keys(RAW_BUCKET, prefix=snapshot_prefix(feed, hour)) or [])
        keys = [k for k in keys if feed_ts_from_key(k) is not None]

        stats = {"feed": feed, "hour_utc": hour_partition(hour), "snapshots": len(keys), "rows": 0,
                 "bad_snapshots": 0, "output": None}
        if not keys:
            return stats

        flatten = FLATTENERS[feed]
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, "part-000.parquet")
            # Stream snapshot by snapshot so memory stays bounded (a trip_updates hour is ~600k rows).
            with pq.ParquetWriter(local, SCHEMAS[feed], compression="zstd") as writer:
                for key in keys:
                    payload = hook.get_key(key, RAW_BUCKET).get()["Body"].read()
                    try:
                        rows = list(flatten(parse_feed(payload)))
                    except Exception as exc:  # one corrupt snapshot shouldn't sink the hour
                        print(f"Skipping unreadable snapshot {key}: {exc!r}")
                        stats["bad_snapshots"] += 1
                        continue
                    if rows:
                        writer.write_table(to_table(feed, rows))
                        stats["rows"] += len(rows)
            out_key = parquet_key(feed, hour)
            hook.load_file(local, out_key, RAW_BUCKET, replace=True)  # replace=True -> re-runs are idempotent
        stats["output"] = f"s3://{RAW_BUCKET}/{out_key}"
        return stats

    @task(outlets=[RAW_RT_PARQUET])  # success -> triggers spark_curate_rt
    def check_and_record(results: list[dict], data_interval_start=None) -> dict:
        results = [dict(r) for r in results]  # mapped XComs arrive as a lazy sequence; materialize them
        problems = []
        for r in results:
            expected = 3600 / EXPECTED_INTERVAL_S[r["feed"]]
            r["completeness"] = round(r["snapshots"] / expected, 3)
            if r["snapshots"] == 0:
                problems.append(f"{r['feed']}: no snapshots (is rt-poller running?)")
            elif r["completeness"] < MIN_COMPLETENESS:
                problems.append(f"{r['feed']}: only {r['snapshots']} of ~{expected:.0f} snapshots")

        manifest = {"hour_utc": hour_partition(data_interval_start), "feeds": results, "problems": problems}
        S3Hook(aws_conn_id=LAKE_CONN_ID).load_string(
            json.dumps(manifest, indent=2), manifest_key(data_interval_start), RAW_BUCKET, replace=True
        )
        if any("no snapshots" in p for p in problems):
            raise ValueError("; ".join(problems))  # fail loudly: capture is down
        for p in problems:
            print(f"WARNING: {p}")  # partial gaps are logged and recorded, not fatal
        return manifest

    results = compact.expand(feed=list(EXPECTED_INTERVAL_S))
    wait_for_late_files >> results
    check_and_record(results)


gtfs_rt_hourly()
