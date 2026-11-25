from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from atl_transit.offline_window import OfflineWindow
from atl_transit.rt_poller import Poller

ROME = "Europe/Rome"  # CET = UTC+1 in November


def at_utc(h: int, m: int = 0) -> datetime:
    return datetime(2026, 11, 25, h, m, tzinfo=UTC)


def test_empty_spec_means_no_window():
    assert OfflineWindow.parse("", ROME) is None
    assert OfflineWindow.parse("  ", ROME) is None


@pytest.mark.parametrize("bad", ["00:00", "7am-9am", "00:00-07:00-08:00", "07:00-07:00"])
def test_bad_specs_are_rejected(bad):
    with pytest.raises(ValueError):
        OfflineWindow.parse(bad, ROME)


def test_window_is_in_local_time():
    w = OfflineWindow.parse("00:00-07:00", ROME)
    assert w.contains(at_utc(23, 0))       # 00:00 Rome
    assert w.contains(at_utc(5, 59))       # 06:59 Rome
    assert not w.contains(at_utc(6, 0))    # 07:00 Rome: window over
    assert not w.contains(at_utc(22, 59))  # 23:59 Rome


def test_window_can_wrap_midnight():
    w = OfflineWindow.parse("23:30-06:00", ROME)
    assert w.contains(at_utc(22, 45)) and w.contains(at_utc(4, 0))
    assert not w.contains(at_utc(5, 0)) and not w.contains(at_utc(21, 0))


def test_grace_after_extends_only_the_end():
    w = OfflineWindow.parse("00:00-07:00", ROME)
    grace = timedelta(minutes=15)
    assert w.contains(at_utc(6, 10), grace_after=grace)      # 07:10 Rome
    assert not w.contains(at_utc(6, 20), grace_after=grace)  # 07:20 Rome
    assert not w.contains(at_utc(22, 50), grace_after=grace)  # 23:50 Rome: before the start


def test_from_env(monkeypatch):
    monkeypatch.setenv("OFFLINE_WINDOW", "00:00-07:00")
    monkeypatch.setenv("OFFLINE_WINDOW_TZ", ROME)
    assert str(OfflineWindow.from_env()) == "00:00-07:00 Europe/Rome"
    monkeypatch.delenv("OFFLINE_WINDOW")
    assert OfflineWindow.from_env() is None


def test_poller_pauses_in_window_and_polls_right_after(tmp_path: Path):
    p = Poller(s3=None, bucket="raw", intervals={"vehicle_positions": 30}, heartbeat=tmp_path / "hb",
               session=type("S", (), {"headers": {}})(), offline=OfflineWindow.parse("00:00-07:00", ROME))
    p.next_due["vehicle_positions"] = 1e12      # pretend a long backoff was pending
    assert p.offline_now(at_utc(1, 0)) is True
    assert p.offline_now(at_utc(6, 1)) is False  # 07:01 Rome
    assert p.next_due == {"vehicle_positions": 0.0}  # due immediately once the window ends


def test_poller_without_window_is_never_offline(tmp_path: Path):
    p = Poller(s3=None, bucket="raw", intervals={"vehicle_positions": 30}, heartbeat=tmp_path / "hb",
               session=type("S", (), {"headers": {}})())
    assert p.offline_now(at_utc(1, 0)) is False
