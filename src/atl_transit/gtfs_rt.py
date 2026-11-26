"""GTFS-realtime helpers: feed registry, lake key layout, protobuf decoding and flattening.

Shared by the poller container (writes raw snapshots) and the Airflow compaction DAG
(turns snapshots into Parquet). No Airflow imports, so it is unit-testable on its own.
"""

from __future__ import annotations

import gzip
import re
from collections.abc import Iterator
from datetime import UTC, datetime

import pyarrow as pa
from google.transit import gtfs_realtime_pb2

MARTA_RT_BASE = "https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService"
FEEDS: dict[str, str] = {
    "vehicle_positions": f"{MARTA_RT_BASE}/vehicle/vehiclepositions.pb",
    "trip_updates": f"{MARTA_RT_BASE}/tripupdate/tripupdates.pb",
}
USER_AGENT = "atl-transit-pipeline/0.1 (+https://github.com/cursorcanis/spark-airflow-data-analyst-portfolio)"

SNAPSHOT_PREFIX = "gtfs_rt"          # raw protobuf snapshots (gzip)
PARQUET_PREFIX = "gtfs_rt_parquet"   # hourly compacted Parquet


# ---------------------------------------------------------------- lake layout
def hour_partition(ts: datetime) -> str:
    ts = ts.astimezone(UTC)
    return f"date={ts:%Y-%m-%d}/hour={ts:%H}"


def snapshot_key(feed: str, feed_ts: int) -> str:
    """Key for one raw snapshot, partitioned by the feed's own UTC timestamp."""
    ts = datetime.fromtimestamp(feed_ts, UTC)
    return f"{SNAPSHOT_PREFIX}/feed={feed}/{hour_partition(ts)}/{feed}_{feed_ts}.pb.gz"


def snapshot_prefix(feed: str, hour: datetime) -> str:
    return f"{SNAPSHOT_PREFIX}/feed={feed}/{hour_partition(hour)}/"


def parquet_key(feed: str, hour: datetime) -> str:
    return f"{PARQUET_PREFIX}/feed={feed}/{hour_partition(hour)}/part-000.parquet"


MANIFEST_PREFIX = f"{PARQUET_PREFIX}/_manifests/"


def manifest_key(hour: datetime) -> str:
    return f"{MANIFEST_PREFIX}{hour_partition(hour)}.json"


_TS_IN_KEY = re.compile(r"_(\d+)\.pb\.gz$")


def feed_ts_from_key(key: str) -> int | None:
    m = _TS_IN_KEY.search(key)
    return int(m.group(1)) if m else None


def purgeable_keys(keys: list[str], compacted_hours: set[str], cutoff: datetime) -> tuple[list[str], int]:
    """Split snapshot keys into (deletable, kept_because_not_compacted).

    A snapshot is deletable when it is older than `cutoff` AND its hour has a manifest
    (`compacted_hours` holds ids like 'date=2026-11-22/hour=18').
    """
    delete, kept = [], 0
    for key in keys:
        feed_ts = feed_ts_from_key(key)
        if feed_ts is None:
            continue
        ts = datetime.fromtimestamp(feed_ts, UTC)
        if ts >= cutoff:
            continue
        if hour_partition(ts) in compacted_hours:
            delete.append(key)
        else:
            kept += 1
    return delete, kept


# ---------------------------------------------------------------- decoding
def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    if payload[:2] == b"\x1f\x8b":  # gzip magic number
        payload = gzip.decompress(payload)
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(payload)
    return msg


def _opt(obj, field: str):
    """Return a proto2 optional field's value, or None when it is not set."""
    return getattr(obj, field) if obj.HasField(field) else None


def _enum_name(obj, field: str, enum_type) -> str | None:
    return enum_type.Name(getattr(obj, field)) if obj.HasField(field) else None


VEHICLE_POSITIONS_SCHEMA = pa.schema([
    ("feed_ts", pa.int64()),
    ("entity_id", pa.string()),
    ("vehicle_id", pa.string()),
    ("vehicle_label", pa.string()),
    ("trip_id", pa.string()),
    ("route_id", pa.string()),
    ("direction_id", pa.int32()),
    ("start_date", pa.string()),
    ("start_time", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("bearing", pa.float32()),
    ("speed_mps", pa.float32()),
    ("current_stop_sequence", pa.int32()),
    ("stop_id", pa.string()),
    ("current_status", pa.string()),
    ("occupancy_status", pa.string()),
    ("vehicle_ts", pa.int64()),
])

TRIP_UPDATES_SCHEMA = pa.schema([
    ("feed_ts", pa.int64()),
    ("trip_id", pa.string()),
    ("route_id", pa.string()),
    ("direction_id", pa.int32()),
    ("start_date", pa.string()),
    ("start_time", pa.string()),
    ("trip_schedule_relationship", pa.string()),
    ("vehicle_id", pa.string()),
    ("trip_update_ts", pa.int64()),
    ("stop_sequence", pa.int32()),
    ("stop_id", pa.string()),
    ("arrival_time", pa.int64()),
    ("arrival_delay", pa.int32()),
    ("departure_time", pa.int64()),
    ("departure_delay", pa.int32()),
    ("stop_schedule_relationship", pa.string()),
])

SCHEMAS = {"vehicle_positions": VEHICLE_POSITIONS_SCHEMA, "trip_updates": TRIP_UPDATES_SCHEMA}

_VP = gtfs_realtime_pb2.VehiclePosition
_TD = gtfs_realtime_pb2.TripDescriptor
_STU = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate


def _trip_fields(trip) -> dict:
    return {
        "trip_id": _opt(trip, "trip_id"),
        "route_id": _opt(trip, "route_id"),
        "direction_id": _opt(trip, "direction_id"),
        "start_date": _opt(trip, "start_date"),
        "start_time": _opt(trip, "start_time"),
    }


def flatten_vehicle_positions(msg: gtfs_realtime_pb2.FeedMessage) -> Iterator[dict]:
    feed_ts = msg.header.timestamp
    for ent in msg.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        pos = v.position if v.HasField("position") else None
        trip = _trip_fields(v.trip) if v.HasField("trip") else {}
        yield {
            "feed_ts": feed_ts,
            "entity_id": ent.id,
            "vehicle_id": _opt(v.vehicle, "id") if v.HasField("vehicle") else None,
            "vehicle_label": _opt(v.vehicle, "label") if v.HasField("vehicle") else None,
            "trip_id": trip.get("trip_id"),
            "route_id": trip.get("route_id"),
            "direction_id": trip.get("direction_id"),
            "start_date": trip.get("start_date"),
            "start_time": trip.get("start_time"),
            "latitude": pos.latitude if pos else None,
            "longitude": pos.longitude if pos else None,
            "bearing": _opt(pos, "bearing") if pos else None,
            "speed_mps": _opt(pos, "speed") if pos else None,
            "current_stop_sequence": _opt(v, "current_stop_sequence"),
            "stop_id": _opt(v, "stop_id"),
            "current_status": _enum_name(v, "current_status", _VP.VehicleStopStatus),
            "occupancy_status": _enum_name(v, "occupancy_status", _VP.OccupancyStatus),
            "vehicle_ts": _opt(v, "timestamp"),
        }


def flatten_trip_updates(msg: gtfs_realtime_pb2.FeedMessage) -> Iterator[dict]:
    """One row per (trip, stop) prediction in the snapshot."""
    feed_ts = msg.header.timestamp
    for ent in msg.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        trip = _trip_fields(tu.trip)
        base = {
            "feed_ts": feed_ts,
            **trip,
            "trip_schedule_relationship": _enum_name(tu.trip, "schedule_relationship", _TD.ScheduleRelationship),
            "vehicle_id": _opt(tu.vehicle, "id") if tu.HasField("vehicle") else None,
            "trip_update_ts": _opt(tu, "timestamp"),
        }
        for stu in tu.stop_time_update:
            arr = stu.arrival if stu.HasField("arrival") else None
            dep = stu.departure if stu.HasField("departure") else None
            yield {
                **base,
                "stop_sequence": _opt(stu, "stop_sequence"),
                "stop_id": _opt(stu, "stop_id"),
                "arrival_time": _opt(arr, "time") if arr else None,
                "arrival_delay": _opt(arr, "delay") if arr else None,
                "departure_time": _opt(dep, "time") if dep else None,
                "departure_delay": _opt(dep, "delay") if dep else None,
                "stop_schedule_relationship": _enum_name(stu, "schedule_relationship", _STU.ScheduleRelationship),
            }


FLATTENERS = {"vehicle_positions": flatten_vehicle_positions, "trip_updates": flatten_trip_updates}


def to_table(feed: str, rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCHEMAS[feed])


# ---------------------------------------------------------------- capture completeness
def assess_capture(results: list[dict], expected_interval_s: dict[str, int], offline_fraction: float = 0.0,
                   min_completeness: float = 0.8) -> tuple[list[dict], list[str], bool]:
    """Score one compacted hour. Returns (results with completeness, problems, fatal).

    offline_fraction = share of the hour inside the known offline window (no internet expected). The expected
    snapshot count shrinks accordingly; an hour that is entirely offline is recorded, not failed. Zero snapshots
    when data WAS expected is fatal (capture is down); a partial gap is only a warning.
    """
    scored, problems = [], []
    online_share = max(0.0, 1.0 - offline_fraction)
    for r in results:
        r = dict(r)
        expected = 3600 / expected_interval_s[r["feed"]] * online_share
        r["expected_snapshots"] = round(expected, 1)
        r["completeness"] = round(r["snapshots"] / expected, 3) if expected >= 1 else None
        if expected < 1:
            pass  # fully inside the offline window: nothing was expected
        elif r["snapshots"] == 0:
            problems.append(f"{r['feed']}: no snapshots (is rt-poller running?)")
        elif r["completeness"] < min_completeness:
            problems.append(f"{r['feed']}: only {r['snapshots']} of ~{expected:.0f} snapshots")
        scored.append(r)
    fatal = any("no snapshots" in p for p in problems)
    return scored, problems, fatal
