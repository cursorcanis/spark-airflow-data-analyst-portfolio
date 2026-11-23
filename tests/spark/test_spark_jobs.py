"""Unit tests for the pure PySpark transformations, using a local SparkSession (needs Java 17+).

Run: pytest tests/spark      (skipped automatically when pyspark or Java is missing)
"""

import shutil
from datetime import UTC, date, datetime

import pytest

pyspark = pytest.importorskip("pyspark")
if not shutil.which("java"):
    pytest.skip("Java not installed", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from atl_transit.spark_jobs import (  # noqa: E402
    gtfs_time_to_seconds,
    prepare_static_table,
    prepare_stop_arrivals,
    prepare_vehicle_positions,
)


def ts(y, mo, d, h, mi=0, s=0) -> int:
    return int(datetime(y, mo, d, h, mi, s, tzinfo=UTC).timestamp())


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_gtfs_time_to_seconds_handles_hours_past_midnight(spark):
    df = spark.createDataFrame([("25:10:05",), (" 08:00:00",)], ["t"])
    assert [r.s for r in df.select(gtfs_time_to_seconds(F.col("t")).alias("s")).collect()] == [90605, 28800]


def test_prepare_static_table_types_and_missing_columns(spark):
    raw = spark.createDataFrame([("T1", "08:00:00", "08:01:00", "S1", "3")],
                                ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])
    out = prepare_static_table("stop_times", raw, "2026-11-22").collect()[0]
    assert out.stop_sequence == 3 and out.arrival_secs == 28800 and out.departure_secs == 28860
    assert out.timepoint is None                       # column absent in feed -> typed NULL
    assert out.feed_ingest_date == date(2026, 11, 22)

    cal = spark.createDataFrame([("WK", "1", "20260101", "20261231")], ["service_id", "monday", "start_date", "end_date"])
    c = prepare_static_table("calendar", cal, "2026-11-22").collect()[0]
    assert c.start_date == date(2026, 1, 1) and c.monday == 1 and c.sunday is None


# Explicit schema: columns that are all NULL in a test can't be type-inferred.
VP_SCHEMA = ("feed_ts long, vehicle_id string, vehicle_label string, trip_id string, route_id string, "
             "direction_id int, latitude double, longitude double, bearing float, speed_mps float, vehicle_ts long")


def test_prepare_vehicle_positions_dedupes_and_rejects(spark):
    t0 = ts(2026, 11, 23, 3, 30)  # 03:30 UTC = 22:30 on Nov 22 in Atlanta
    rows = [
        (t0 + 10, "2303", "1603", "T1", "R1", None, 33.84, -84.25, 52.0, None, t0),
        (t0 + 40, "2303", "1603", "T1", "R1", None, 33.84, -84.25, 52.0, None, t0),     # same fix, next snapshot
        (t0 + 40, "2400", "1700", "T2", "R2", None, 40.71, -74.00, 0.0, None, t0),      # outside Atlanta
        (t0 + 40, None, None, "T3", "R3", None, 33.75, -84.39, 0.0, None, t0),          # no vehicle id
    ]
    raw = spark.createDataFrame(rows, VP_SCHEMA)
    clean, rejected = prepare_vehicle_positions(raw)
    out = clean.collect()
    assert len(out) == 1 and out[0].feed_ts == t0 + 10                 # earliest sighting kept
    assert out[0].event_date == date(2026, 11, 22)                     # local (Atlanta) date
    assert rejected.count() == 2


TU_COLS = ["feed_ts", "trip_id", "route_id", "start_date", "vehicle_id", "stop_sequence", "stop_id", "arrival_time"]
ST_COLS = ["trip_id", "stop_sequence", "stop_id", "arrival_secs"]


def test_prepare_stop_arrivals_latest_passed_report_and_delay(spark):
    sched_ts = ts(2026, 11, 23, 13, 0)            # 08:00 EST = 13:00 UTC
    tu = spark.createDataFrame([
        (sched_ts + 60, "T1", "R1", "20261123", "V1", 5, "9997", sched_ts + 90),      # predicted, not yet passed
        (sched_ts + 150, "T1", "R1", "20261123", "V1", 5, "9997", sched_ts + 110),    # passed: first report
        (sched_ts + 180, "T1", "R1", "20261123", "V1", 5, "9997", sched_ts + 120),    # passed: latest report wins
        (sched_ts + 180, "T1", "R1", "20261123", "V1", 6, "9998", sched_ts + 600),    # future stop -> excluded
        (sched_ts + 180, "TX", "R9", "20261123", "V9", 1, "1234", sched_ts),          # trip not in schedule
    ], TU_COLS)
    st = spark.createDataFrame([("T1", 5, "62054", 8 * 3600), ("T1", 6, "62116", 8 * 3600 + 600)], ST_COLS)

    out = {r.trip_id: r for r in prepare_stop_arrivals(tu, st).collect()}
    assert set(out) == {"T1", "TX"}
    r = out["T1"]
    assert r.stop_sequence == 5 and r.delay_s == 120 and r.feed_ts == sched_ts + 180
    assert r.stop_id == "62054" and r.rt_stop_id == "9997"             # schedule's stop id preferred
    assert r.scheduled_arrival_ts == datetime(2026, 11, 23, 13, 0)
    assert out["TX"].has_schedule is False and out["TX"].stop_id == "1234"


def test_scheduled_time_is_correct_on_dst_change_day(spark):
    # US DST starts 2026-03-08 at 02:00. 10:00 local that day is EDT (UTC-4) -> 14:00 UTC.
    obs = ts(2026, 3, 8, 14, 0)
    tu = spark.createDataFrame([(obs + 30, "T1", "R1", "20260308", "V1", 1, "x", obs)], TU_COLS)
    st = spark.createDataFrame([("T1", 1, "S1", 10 * 3600)], ST_COLS)
    r = prepare_stop_arrivals(tu, st).collect()[0]
    assert r.scheduled_arrival_ts == datetime(2026, 3, 8, 14, 0) and r.delay_s == 0
