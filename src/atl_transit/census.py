"""Census tract boundaries for metro Atlanta from the Census Bureau's TIGERweb REST API (no key needed)."""

from __future__ import annotations

import json
from collections.abc import Iterator

import requests

TIGERWEB_TRACTS_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/0/query"
GEORGIA_FIPS = "13"
# Atlanta Regional Commission (ARC) 11-county region; MARTA operates in Fulton, DeKalb and Clayton.
ARC_COUNTIES = {
    "057": "Cherokee", "063": "Clayton", "067": "Cobb", "089": "DeKalb", "097": "Douglas", "113": "Fayette",
    "117": "Forsyth", "121": "Fulton", "135": "Gwinnett", "151": "Henry", "247": "Rockdale",
}
OUT_FIELDS = "GEOID,STATE,COUNTY,TRACT,NAME,BASENAME,AREALAND,AREAWATER,INTPTLAT,INTPTLON"
PAGE_SIZE = 200


def where_clause(counties=ARC_COUNTIES) -> str:
    county_list = ",".join(f"'{c}'" for c in sorted(counties))
    return f"STATE='{GEORGIA_FIPS}' AND COUNTY IN ({county_list})"


def count_tracts(session: requests.Session) -> int:
    resp = session.get(TIGERWEB_TRACTS_URL, params={"where": where_clause(), "returnCountOnly": "true", "f": "json"},
                       timeout=60)
    resp.raise_for_status()
    return int(resp.json()["count"])


def fetch_tracts(session: requests.Session, page_size: int = PAGE_SIZE) -> Iterator[dict]:
    """Yield GeoJSON features page by page (ArcGIS caps records per request)."""
    offset = 0
    while True:
        resp = session.get(TIGERWEB_TRACTS_URL, params={
            "where": where_clause(), "outFields": OUT_FIELDS, "outSR": "4326", "f": "geojson",
            "orderByFields": "GEOID", "resultOffset": offset, "resultRecordCount": page_size,
            "geometryPrecision": 6,
        }, timeout=120)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        yield from features
        if len(features) < page_size:
            return
        offset += page_size


def feature_to_row(feature: dict) -> tuple:
    """Map a TIGERweb GeoJSON feature to a row for raw.census_tracts."""
    p = feature["properties"]
    return (
        p["GEOID"], p["STATE"], p["COUNTY"], ARC_COUNTIES.get(p["COUNTY"]), p["TRACT"], p["NAME"],
        int(p["AREALAND"]), int(p["AREAWATER"]), float(p["INTPTLAT"]), float(p["INTPTLON"]),
        json.dumps(feature["geometry"]),
    )
