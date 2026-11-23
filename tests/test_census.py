import json

import requests

from atl_transit.census import (
    ACS_VARIABLES,
    ARC_COUNTIES,
    CensusApiError,
    acs_rows,
    feature_to_row,
    fetch_acs_tracts,
    fetch_tracts,
    where_clause,
)

FEATURE = {
    "type": "Feature",
    "properties": {"GEOID": "13121001100", "STATE": "13", "COUNTY": "121", "TRACT": "001100",
                   "NAME": "Census Tract 11", "BASENAME": "11", "AREALAND": 1234567, "AREAWATER": 0,
                   "INTPTLAT": "+33.7849", "INTPTLON": "-084.3838"},
    "geometry": {"type": "Polygon", "coordinates": [[[-84.39, 33.78], [-84.38, 33.78], [-84.38, 33.79],
                                                     [-84.39, 33.78]]]},
}


def test_where_clause_covers_arc_region():
    clause = where_clause()
    assert clause.startswith("STATE='13' AND COUNTY IN (")
    assert all(f"'{c}'" in clause for c in ARC_COUNTIES)
    assert len(ARC_COUNTIES) == 11


def test_feature_to_row():
    row = feature_to_row(FEATURE)
    assert row[0] == "13121001100"
    assert row[3] == "Fulton"
    assert row[8] == 33.7849 and row[9] == -84.3838   # "+033.." strings parsed to floats
    assert json.loads(row[10])["type"] == "Polygon"


class PagedSession:
    """Serves 5 features in pages, like the ArcGIS API."""

    def __init__(self, total):
        self.features = [{**FEATURE, "properties": {**FEATURE["properties"], "GEOID": str(i)}} for i in range(total)]
        self.calls = 0

    def get(self, url, params, timeout):
        self.calls += 1
        start, size = params["resultOffset"], params["resultRecordCount"]
        page = self.features[start:start + size]

        class R:
            def raise_for_status(self):
                pass

            def json(self):
                return {"features": page}

        return R()


def test_fetch_tracts_pages_until_short_page():
    session = PagedSession(total=5)
    got = list(fetch_tracts(session, page_size=2))
    assert [f["properties"]["GEOID"] for f in got] == ["0", "1", "2", "3", "4"]
    assert session.calls == 3


def test_acs_rows_builds_geoid_and_nulls_sentinels():
    header = ["NAME", *ACS_VARIABLES, "state", "county", "tract"]
    values = ["2694", "-666666666", "1615", "226", "1236", "0", "52", "2694", "212"]
    table = [header, ["Census Tract 218.16; DeKalb County; Georgia", *values, "13", "089", "021816"]]
    (row,) = acs_rows(table, 2024)
    assert row[0] == "13089021816" and row[1] == 2024
    assert row[2] == 2694            # total_population
    assert row[3] is None            # median income sentinel -> NULL
    assert row[6] == 1236


def test_census_errors_never_include_the_key():
    class Boom:
        def get(self, *a, **k):
            raise requests.ConnectionError("failed for https://api.census.gov/...&key=SECRET123")

    try:
        fetch_acs_tracts(Boom(), "SECRET123", 2024)
    except CensusApiError as exc:
        assert "SECRET123" not in str(exc) and exc.__cause__ is None
    else:
        raise AssertionError("expected CensusApiError")
