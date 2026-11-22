import json

from atl_transit.census import ARC_COUNTIES, feature_to_row, fetch_tracts, where_clause

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
