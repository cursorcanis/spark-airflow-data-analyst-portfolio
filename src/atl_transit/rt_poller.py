"""Long-running poller: fetches MARTA GTFS-realtime feeds and lands raw snapshots in the lake.

Why a separate service instead of an Airflow task: feeds refresh every ~30 s. Airflow is built
for batch orchestration (minutes to days); scheduling a DAG run every 30 s would flood its
metadata DB. The poller does the high-frequency capture; Airflow compacts the snapshots hourly.

Run: python -m atl_transit.rt_poller   (configured entirely through environment variables)
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

from atl_transit.gtfs_rt import FEEDS, USER_AGENT, parse_feed, snapshot_key

log = logging.getLogger("rt_poller")


class Poller:
    def __init__(self, s3, bucket: str, intervals: dict[str, int], heartbeat: Path, session=None):
        self.s3 = s3
        self.bucket = bucket
        self.intervals = intervals
        self.heartbeat = heartbeat
        self.http = session or requests.Session()
        self.http.headers["User-Agent"] = USER_AGENT
        self.last_ts: dict[str, int] = {}
        self.next_due: dict[str, float] = dict.fromkeys(intervals, 0.0)
        self.failures: dict[str, int] = dict.fromkeys(intervals, 0)
        self.running = True

    def ensure_bucket(self) -> None:
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.s3.create_bucket(Bucket=self.bucket)

    def poll_once(self, feed: str) -> str:
        """Fetch one feed; returns 'stored', 'unchanged' or raises."""
        resp = self.http.get(FEEDS[feed], timeout=(5, 30))
        resp.raise_for_status()
        payload = resp.content
        feed_ts = parse_feed(payload).header.timestamp  # also proves the payload is valid protobuf
        if not feed_ts:
            raise ValueError("feed header has no timestamp")
        if self.last_ts.get(feed) == feed_ts:
            return "unchanged"  # server hasn't refreshed yet; avoid duplicate snapshots
        self.s3.put_object(
            Bucket=self.bucket,
            Key=snapshot_key(feed, feed_ts),
            Body=gzip.compress(payload),
            ContentType="application/x-protobuf",
            ContentEncoding="gzip",
        )
        self.last_ts[feed] = feed_ts
        return "stored"

    def run(self) -> None:
        self.ensure_bucket()
        log.info(json.dumps({"event": "start", "feeds": self.intervals, "bucket": self.bucket}))
        while self.running:
            now = time.monotonic()
            for feed, interval in self.intervals.items():
                if now < self.next_due[feed]:
                    continue
                try:
                    status = self.poll_once(feed)
                    self.failures[feed] = 0
                    self.next_due[feed] = now + interval
                    log.info(json.dumps({"event": status, "feed": feed, "feed_ts": self.last_ts.get(feed)}))
                except Exception as exc:  # keep running; back off exponentially, capped at 5 min
                    self.failures[feed] += 1
                    backoff = min(interval * 2 ** self.failures[feed], 300)
                    self.next_due[feed] = now + backoff
                    log.warning(json.dumps({"event": "error", "feed": feed, "error": repr(exc)[:300],
                                            "retry_in_s": backoff}))
            self.heartbeat.write_text(str(int(time.time())))
            time.sleep(1)
        log.info(json.dumps({"event": "stop"}))

    def stop(self, *_):
        self.running = False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("LAKE_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["LAKE_ACCESS_KEY"],
        aws_secret_access_key=os.environ["LAKE_SECRET_KEY"],
        region_name=os.environ.get("LAKE_REGION", "us-east-1"),
    )
    poller = Poller(
        s3=s3,
        bucket=os.environ.get("LAKE_RAW_BUCKET", "raw"),
        intervals={
            "vehicle_positions": int(os.environ.get("POLL_VEHICLE_POSITIONS_S", "30")),
            "trip_updates": int(os.environ.get("POLL_TRIP_UPDATES_S", "120")),
        },
        heartbeat=Path(os.environ.get("HEARTBEAT_FILE", "/tmp/heartbeat")),
    )
    signal.signal(signal.SIGTERM, poller.stop)  # `docker compose stop` sends SIGTERM
    signal.signal(signal.SIGINT, poller.stop)
    poller.run()


if __name__ == "__main__":
    main()
