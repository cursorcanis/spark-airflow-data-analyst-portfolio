import gzip
from datetime import UTC, datetime, timedelta

from google.transit import gtfs_realtime_pb2 as rt

from atl_transit.gtfs_rt import (
    feed_ts_from_key,
    flatten_trip_updates,
    flatten_vehicle_positions,
    parse_feed,
    purgeable_keys,
    snapshot_key,
    to_table,
)
from atl_transit.rt_poller import Poller

FEED_TS = 1795387150  # 2026-11-22 22:39:10 UTC (17:39 in Atlanta)


def vehicle_feed() -> rt.FeedMessage:
    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = FEED_TS
    ent = msg.entity.add(id="1603")
    ent.vehicle.trip.trip_id = "11713987"
    ent.vehicle.trip.route_id = "29163"
    ent.vehicle.trip.start_date = "20261122"
    ent.vehicle.position.latitude = 33.84552
    ent.vehicle.position.longitude = -84.2553864
    ent.vehicle.position.bearing = 52
    ent.vehicle.vehicle.id = "2303"
    ent.vehicle.vehicle.label = "1603"
    ent.vehicle.timestamp = FEED_TS - 17
    msg.entity.add(id="no-vehicle")  # entity without a vehicle payload is ignored
    return msg


def trip_update_feed() -> rt.FeedMessage:
    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = FEED_TS
    tu = msg.entity.add(id="11713987").trip_update
    tu.trip.trip_id = "11713987"
    tu.trip.route_id = "29163"
    tu.vehicle.id = "2303"
    for seq, stop, t in [(27, "99974003", FEED_TS - 295), (28, "99973310", FEED_TS + 60)]:
        stu = tu.stop_time_update.add(stop_sequence=seq, stop_id=stop)
        stu.arrival.time = t
        stu.departure.time = t
    return msg


def test_flatten_vehicle_positions():
    rows = list(flatten_vehicle_positions(vehicle_feed()))
    assert len(rows) == 1
    r = rows[0]
    assert r["vehicle_id"] == "2303" and r["route_id"] == "29163"
    assert abs(r["latitude"] - 33.84552) < 1e-5
    assert r["speed_mps"] is None          # unset optional field -> None, not 0
    assert r["direction_id"] is None
    assert to_table("vehicle_positions", rows).num_rows == 1


def test_flatten_trip_updates_one_row_per_stop():
    rows = list(flatten_trip_updates(trip_update_feed()))
    assert [r["stop_sequence"] for r in rows] == [27, 28]
    assert rows[0]["arrival_time"] == FEED_TS - 295
    assert rows[0]["arrival_delay"] is None
    assert all(r["vehicle_id"] == "2303" and r["feed_ts"] == FEED_TS for r in rows)
    assert to_table("trip_updates", rows).num_rows == 2


def test_parse_feed_accepts_gzip():
    raw = vehicle_feed().SerializeToString()
    assert parse_feed(gzip.compress(raw)).header.timestamp == FEED_TS
    assert parse_feed(raw).header.timestamp == FEED_TS


def test_snapshot_key_layout_and_roundtrip():
    key = snapshot_key("vehicle_positions", FEED_TS)
    assert key == "gtfs_rt/feed=vehicle_positions/date=2026-11-22/hour=22/vehicle_positions_1795387150.pb.gz"
    assert feed_ts_from_key(key) == FEED_TS


def test_purgeable_keys_only_old_and_compacted():
    old = int((datetime(2026, 11, 1, 10, tzinfo=UTC)).timestamp())
    old_uncompacted = int((datetime(2026, 11, 1, 11, tzinfo=UTC)).timestamp())
    keys = [snapshot_key("trip_updates", t) for t in (old, old_uncompacted, FEED_TS)]
    cutoff = datetime.fromtimestamp(FEED_TS, UTC) - timedelta(days=14)
    delete, kept = purgeable_keys(keys, {"date=2026-11-01/hour=10"}, cutoff)
    assert delete == [keys[0]]
    assert kept == 1                         # old but never compacted -> kept


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body


class FakeResponse:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.headers = {}

    def get(self, url, timeout):
        return FakeResponse(self.payload)


def test_poller_skips_unchanged_snapshots(tmp_path):
    s3 = FakeS3()
    session = FakeSession(vehicle_feed().SerializeToString())
    poller = Poller(s3, "raw", {"vehicle_positions": 30}, tmp_path / "hb", session=session)
    assert poller.poll_once("vehicle_positions") == "stored"
    assert poller.poll_once("vehicle_positions") == "unchanged"   # same header timestamp
    assert list(s3.objects) == [snapshot_key("vehicle_positions", FEED_TS)]
    assert parse_feed(next(iter(s3.objects.values()))).header.timestamp == FEED_TS
    assert "atl-transit-pipeline" in session.headers["User-Agent"]


INTERVALS = {"vehicle_positions": 30, "trip_updates": 120}


def _hour(vp: int, tu: int) -> list[dict]:
    return [{"feed": "vehicle_positions", "snapshots": vp}, {"feed": "trip_updates", "snapshots": tu}]


def test_assess_capture_full_hour_ok():
    from atl_transit.gtfs_rt import assess_capture
    scored, problems, fatal = assess_capture(_hour(118, 30), INTERVALS)
    assert not problems and not fatal and scored[0]["completeness"] == 0.983


def test_assess_capture_outage_is_fatal():
    from atl_transit.gtfs_rt import assess_capture
    _, problems, fatal = assess_capture(_hour(0, 0), INTERVALS)
    assert fatal and len(problems) == 2


def test_assess_capture_offline_window_hour_is_expected_empty():
    from atl_transit.gtfs_rt import assess_capture
    scored, problems, fatal = assess_capture(_hour(0, 0), INTERVALS, offline_fraction=1.0)
    assert not problems and not fatal and scored[0]["completeness"] is None


def test_assess_capture_partly_offline_hour_scales_expectation():
    from atl_transit.gtfs_rt import assess_capture
    scored, problems, fatal = assess_capture(_hour(58, 15), INTERVALS, offline_fraction=0.5)
    assert not problems and not fatal and scored[0]["expected_snapshots"] == 60.0
