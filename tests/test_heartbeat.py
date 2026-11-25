from datetime import UTC, datetime, timedelta

import pytest

from atl_transit import heartbeat
from atl_transit.heartbeat import evaluate, manifest_prefixes, newest_manifest_hour, snapshot_prefixes

NOW = datetime(2026, 11, 25, 21, 10, tzinfo=UTC)


def snap_key(ts: datetime) -> str:
    return f"gtfs_rt/feed=vehicle_positions/date={ts:%Y-%m-%d}/hour={ts:%H}/vehicle_positions_{int(ts.timestamp())}.pb.gz"


def manifest_key(hour: datetime) -> str:
    return f"gtfs_rt_parquet/_manifests/date={hour:%Y-%m-%d}/hour={hour:%H}.json"


FRESH_SNAPS = [snap_key(NOW - timedelta(minutes=m)) for m in (1, 30, 70)]
FRESH_MANIFESTS = [manifest_key(datetime(2026, 11, 25, h, tzinfo=UTC)) for h in (18, 19, 20)]


def test_fresh_data_is_ok():
    r = evaluate(NOW, FRESH_SNAPS, FRESH_MANIFESTS)
    assert r.ok, r.problems
    assert r.newest_snapshot_at == datetime.fromtimestamp(int((NOW - timedelta(minutes=1)).timestamp()), UTC)
    assert r.newest_manifest_hour == datetime(2026, 11, 25, 20, tzinfo=UTC)


def test_stale_snapshots_are_a_problem():
    r = evaluate(NOW, [snap_key(NOW - timedelta(minutes=40))], FRESH_MANIFESTS)
    assert not r.ok and len(r.problems) == 1
    assert r.problems[0].startswith("RAW DATA STALE: newest is 40 min old")


def test_no_snapshots_at_all_is_a_problem():
    r = evaluate(NOW, [], FRESH_MANIFESTS)
    assert "no snapshot in the last 2 hours" in r.problems[0]


def test_compaction_behind_is_a_problem():
    r = evaluate(NOW, FRESH_SNAPS, [manifest_key(datetime(2026, 11, 25, 16, tzinfo=UTC))])
    assert len(r.problems) == 1 and r.problems[0].startswith("COMPACTION BEHIND: newest is 5 h behind")


def test_prefixes_cover_hour_and_day_boundaries():
    just_after_midnight = datetime(2026, 11, 26, 0, 5, tzinfo=UTC)
    assert snapshot_prefixes(just_after_midnight) == [
        "gtfs_rt/feed=vehicle_positions/date=2026-11-25/hour=23/",
        "gtfs_rt/feed=vehicle_positions/date=2026-11-26/hour=00/",
    ]
    assert manifest_prefixes(just_after_midnight) == [
        "gtfs_rt_parquet/_manifests/date=2026-11-25/",
        "gtfs_rt_parquet/_manifests/date=2026-11-26/",
    ]


def test_newest_manifest_ignores_other_keys():
    assert newest_manifest_hour(["gtfs_rt_parquet/_manifests/readme.txt"]) is None


class FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.mark.parametrize(("snaps", "expected_url"), [(FRESH_SNAPS, "https://hc.example/abc"), ([], "https://hc.example/abc/fail")])
def test_ping_hits_success_or_fail_endpoint(monkeypatch, snaps, expected_url):
    seen = {}

    def fake_urlopen(req, timeout):
        seen["url"], seen["body"], seen["method"] = req.full_url, req.data.decode(), req.get_method()
        return FakeResponse()

    monkeypatch.setattr(heartbeat.urllib.request, "urlopen", fake_urlopen)
    status = heartbeat.ping("https://hc.example/abc/", evaluate(NOW, snaps, FRESH_MANIFESTS))
    assert status == 200 and seen["url"] == expected_url and seen["method"] == "POST"
    assert seen["body"].startswith("status: ")
