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
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

from atl_transit.gtfs_rt import FEEDS, USER_AGENT, parse_feed, snapshot_key
from atl_transit.offline_window import OfflineWindow

log = logging.getLogger("rt_poller")


class Poller:
    def __init__(self, s3, bucket: str, intervals: dict[str, int], heartbeat: Path, session=None,
                 offline: OfflineWindow | None = None):
        self.s3 = s3
        self.bucket = bucket
        self.intervals = intervals
        self.heartbeat = heartbeat
        self.http = session or requests.Session()
        self.http.headers["User-Agent"] = USER_AGENT
        self.last_ts: dict[str, int] = {}
        self.next_due: dict[str, float] = dict.fromkeys(intervals, 0.0)
        self.failures: dict[str, int] = dict.fromkeys(intervals, 0)
        self.offline = offline
        self.in_offline = False
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

    def offline_now(self, moment: datetime | None = None) -> bool:
        """Inside the configured no-internet window? Logs each transition once."""
        inside = bool(self.offline and self.offline.contains(moment or datetime.now(UTC)))
        if inside != self.in_offline:
            self.in_offline = inside
            log.info(json.dumps({"event": "offline_window_start" if inside else "offline_window_end",
                                 "window": str(self.offline)}))
            if not inside:  # poll everything right away when the window ends
                self.next_due = dict.fromkeys(self.intervals, 0.0)
                self.failures = dict.fromkeys(self.intervals, 0)
        return inside

    def run(self) -> None:
        self.ensure_bucket()
        log.info(json.dumps({"event": "start", "feeds": self.intervals, "bucket": self.bucket,
                             "offline_window": str(self.offline) if self.offline else None}))
        while self.running:
            if self.offline_now():  # no internet expected: don't call MARTA, but stay "healthy"
                self.heartbeat.write_text(str(int(time.time())))
                time.sleep(5)
                continue
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
        offline=OfflineWindow.from_env(),
    )
    signal.signal(signal.SIGTERM, poller.stop)  # `docker compose stop` sends SIGTERM
    signal.signal(signal.SIGINT, poller.stop)
    poller.run()


if __name__ == "__main__":
    main()
