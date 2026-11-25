"""Pipeline heartbeat: is new data still arriving? Reports to an off-box "dead man's switch".

The check runs inside the stack (Airflow, every 15 min), but the alert must come from OUTSIDE it: if the laptop
freezes or loses internet, nothing on it can send an alert. So the check pings an external monitor
(healthchecks.io or any compatible service) when everything is fresh, and pings its /fail endpoint with the
reason when not. The monitor emails you when a ping fails OR when pings stop arriving.

Two signals, both read from the lake:
  * raw snapshots: the newest vehicle-positions snapshot's feed timestamp (poller + MARTA + internet alive)
  * hourly manifests: the newest compacted hour (Airflow scheduling + compaction alive)

No Airflow imports, so it is unit-testable on its own.
"""

from __future__ import annotations

import re
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from atl_transit.gtfs_rt import MANIFEST_PREFIX, feed_ts_from_key, snapshot_prefix

# MARTA's feed updates every ~30 s, 24/7, and the poller stores each new snapshot.
MAX_SNAPSHOT_AGE = timedelta(minutes=15)
# The hour H is compacted shortly after H+1:00, so the newest manifest is normally 1-2 h behind "now".
MAX_MANIFEST_LAG = timedelta(hours=3)
HEARTBEAT_FEED = "vehicle_positions"

_MANIFEST_HOUR = re.compile(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})\.json$")


@dataclass
class HeartbeatResult:
    newest_snapshot_at: datetime | None
    newest_manifest_hour: datetime | None
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems

    def summary(self) -> str:
        def fmt(ts: datetime | None) -> str:
            return f"{ts:%Y-%m-%d %H:%M} UTC" if ts else "none"

        lines = [
            f"status: {'OK' if self.ok else 'PROBLEM'}",
            f"newest raw snapshot: {fmt(self.newest_snapshot_at)}",
            f"newest compacted hour: {fmt(self.newest_manifest_hour)}",
            *self.problems,
        ]
        return "\n".join(lines)


def snapshot_prefixes(now: datetime) -> list[str]:
    """Only the current and previous hour need listing: anything older is stale by definition."""
    return [snapshot_prefix(HEARTBEAT_FEED, now - timedelta(hours=h)) for h in (1, 0)]


def manifest_prefixes(now: datetime) -> list[str]:
    days = {(now - timedelta(hours=h)).strftime("%Y-%m-%d") for h in (0, 24)}
    return [f"{MANIFEST_PREFIX}date={d}/" for d in sorted(days)]


def newest_snapshot(keys: list[str]) -> datetime | None:
    stamps = [ts for k in keys if (ts := feed_ts_from_key(k)) is not None]
    return datetime.fromtimestamp(max(stamps), UTC) if stamps else None


def newest_manifest_hour(keys: list[str]) -> datetime | None:
    hours = [
        datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H").replace(tzinfo=UTC)
        for k in keys
        if (m := _MANIFEST_HOUR.search(k))
    ]
    return max(hours) if hours else None


def evaluate(now: datetime, snapshot_keys: list[str], manifest_keys: list[str]) -> HeartbeatResult:
    snap = newest_snapshot(snapshot_keys)
    manifest = newest_manifest_hour(manifest_keys)
    result = HeartbeatResult(snap, manifest)
    if snap is None or now - snap > MAX_SNAPSHOT_AGE:
        minutes = MAX_SNAPSHOT_AGE // timedelta(minutes=1)
        age = (
            "no snapshot in the last 2 hours" if snap is None
            else f"newest is {(now - snap) // timedelta(minutes=1)} min old"
        )
        result.problems.append(
            f"RAW DATA STALE: {age} (limit {minutes} min). Check rt-poller, the internet connection, and MARTA's feed."
        )
    if manifest is None or now - manifest > MAX_MANIFEST_LAG:
        hours = MAX_MANIFEST_LAG // timedelta(hours=1)
        lag = (
            "no compacted hour in 2 days" if manifest is None
            else f"newest is {(now - manifest) // timedelta(hours=1)} h behind"
        )
        result.problems.append(f"COMPACTION BEHIND: {lag} (limit {hours} h). Check the gtfs_rt_hourly DAG.")
    return result


def ping(base_url: str, result: HeartbeatResult, timeout_s: float = 10) -> int:
    """POST the summary to <base_url> (OK) or <base_url>/fail (problem). Returns the HTTP status."""
    url = base_url.rstrip("/") + ("" if result.ok else "/fail")
    req = urllib.request.Request(url, data=result.summary().encode("utf-8"), method="POST")
    req.add_header("User-Agent", "atl-transit-heartbeat")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 (URL comes from our own config)
        return resp.status
