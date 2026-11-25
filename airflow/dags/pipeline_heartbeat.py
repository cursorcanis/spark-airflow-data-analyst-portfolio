"""Every 15 min: is new data still arriving? Report to an off-box dead man's switch.

Pings HEALTHCHECK_PING_URL (e.g. a healthchecks.io check) when raw snapshots and hourly compaction are fresh,
and <url>/fail with the reason when they aren't. The external monitor alerts when it gets a /fail OR when pings
stop (laptop frozen, internet down, Airflow dead), which no check running on the laptop could report.
Without HEALTHCHECK_PING_URL the DAG still runs and fails visibly in Airflow on stale data.

During OFFLINE_WINDOW (+15 min for data to flow again) the check is skipped: no internet is expected, so stale
data isn't an incident and a ping couldn't get out anyway. Give the external check a matching schedule
(e.g. healthchecks.io cron "*/15 7-23 * * *" in the same time zone) so it doesn't expect pings then.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

from atl_transit.heartbeat import evaluate, manifest_prefixes, ping, snapshot_prefixes
from atl_transit.offline_window import OfflineWindow

LAKE_CONN_ID = "lake_s3"
RAW_BUCKET = os.environ.get("LAKE_RAW_BUCKET", "raw")
RESUME_GRACE = timedelta(minutes=15)   # after the offline window, give the poller time to catch up


@dag(
    dag_id="pipeline_heartbeat",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 11, 25, tz="America/New_York"),
    catchup=False,             # a heartbeat about the past is meaningless
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 0},
    tags=["monitoring"],
    doc_md=__doc__,
)
def pipeline_heartbeat():
    @task
    def check_and_ping() -> dict:
        now = datetime.now(UTC)
        offline = OfflineWindow.from_env()
        if offline and offline.contains(now, grace_after=RESUME_GRACE):
            print(f"inside the offline window {offline} (+{RESUME_GRACE}): no check, no ping")
            return {"ok": True, "skipped": "offline_window"}
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        snapshots = [k for p in snapshot_prefixes(now) for k in hook.list_keys(RAW_BUCKET, prefix=p) or []]
        manifests = [k for p in manifest_prefixes(now) for k in hook.list_keys(RAW_BUCKET, prefix=p) or []]
        result = evaluate(now, snapshots, manifests)
        print(result.summary())

        url = os.environ.get("HEALTHCHECK_PING_URL", "").strip()
        if url:
            print(f"ping -> HTTP {ping(url, result)}")
        else:
            print("HEALTHCHECK_PING_URL not set: no external alert sent")
        if not result.ok:
            raise RuntimeError("; ".join(result.problems))
        return {"ok": True}

    check_and_ping()


pipeline_heartbeat()
