import json

from atl_transit.webmap import LAYERS, SUMMARY_SQL, export_webmap


class FakeCursor:
    def __init__(self, results):
        self.results, self.executed = results, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return (self.results[len(self.executed) - 1],)


class FakeConn:
    def __init__(self, results):
        self.cur = FakeCursor(results)

    def cursor(self):
        return self.cur


def test_every_layer_is_a_feature_collection_with_rounded_coordinates():
    for name, sql in LAYERS.items():
        assert name.endswith(".geojson")
        assert "'FeatureCollection'" in sql and "st_asgeojson(q.geom, 5)" in sql
        assert "marts." in sql                    # the map only ever reads published marts


def test_export_writes_each_file(tmp_path):
    fc = json.dumps({"type": "FeatureCollection", "features": []})
    conn = FakeConn([fc] * len(LAYERS) + ['{"arrivals": 3}'])
    sizes = export_webmap(conn, tmp_path)
    assert set(sizes) == {*LAYERS, "summary.json"}
    assert json.loads((tmp_path / "summary.json").read_text()) == {"arrivals": 3}
    assert conn.cur.executed[-1] == SUMMARY_SQL


def test_export_rejects_invalid_json(tmp_path):
    conn = FakeConn(["not json"])
    try:
        export_webmap(conn, tmp_path)
    except json.JSONDecodeError:
        return
    raise AssertionError("invalid JSON should fail the export")
