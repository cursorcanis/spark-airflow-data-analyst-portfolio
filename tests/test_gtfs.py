import zipfile

import pytest

from atl_transit.gtfs import GtfsValidationError, validate_gtfs_zip

VALID_FILES = {
    "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nMARTA,MARTA,https://itsmarta.com,America/New_York\n",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n1,Five Points,33.7539,-84.3916\n2,Peachtree Center,33.7597,-84.3875\n",
    "routes.txt": "route_id,route_short_name,route_type\nRED,Red,1\n",
    "trips.txt": "route_id,service_id,trip_id\nRED,WK,T1\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,1,1\nT1,08:02:00,08:02:00,2,2\n",
    "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nWK,1,1,1,1,1,0,0,20260101,20261231\n",
    "feed_info.txt": "feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date,feed_version\nMARTA,https://itsmarta.com,en,20260801,20261231,v42\n",
}


def make_zip(tmp_path, files):
    path = tmp_path / "feed.zip"
    with zipfile.ZipFile(path, "w") as zf:
        for name, body in files.items():
            zf.writestr(name, body)
    return path


def test_valid_feed_summary(tmp_path):
    summary = validate_gtfs_zip(make_zip(tmp_path, VALID_FILES))
    assert summary.row_counts["stops.txt"] == 2
    assert summary.row_counts["stop_times.txt"] == 2
    assert summary.feed_version == "v42"
    assert summary.feed_start_date == "20260801"
    assert len(summary.sha256) == 64


def test_missing_required_file(tmp_path):
    files = {k: v for k, v in VALID_FILES.items() if k != "trips.txt"}
    with pytest.raises(GtfsValidationError, match="trips.txt"):
        validate_gtfs_zip(make_zip(tmp_path, files))


def test_needs_a_calendar(tmp_path):
    files = {k: v for k, v in VALID_FILES.items() if k != "calendar.txt"}
    with pytest.raises(GtfsValidationError, match="calendar"):
        validate_gtfs_zip(make_zip(tmp_path, files))


def test_empty_required_table(tmp_path):
    files = {**VALID_FILES, "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"}
    with pytest.raises(GtfsValidationError, match="no rows"):
        validate_gtfs_zip(make_zip(tmp_path, files))


def test_not_a_zip(tmp_path):
    path = tmp_path / "feed.zip"
    path.write_text("<html>maintenance page</html>")
    with pytest.raises(GtfsValidationError, match="not a zip"):
        validate_gtfs_zip(path)
