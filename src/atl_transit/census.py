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


# ---------------------------------------------------------------- ACS 5-year demographics (needs a free API key)
ACS_URL = "https://api.census.gov/data/{year}/acs/acs5"
# Census variable -> our column name. B08301 = means of transportation to work; B25044 = vehicles per household.
ACS_VARIABLES = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B08301_001E": "workers_16_plus",
    "B08301_010E": "workers_public_transit",
    "B25044_001E": "occupied_households",
    "B25044_003E": "owner_households_no_vehicle",
    "B25044_010E": "renter_households_no_vehicle",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "below_poverty",
}


class CensusApiError(RuntimeError):
    """Raised without the request URL, so the API key never ends up in logs."""


def fetch_acs_tracts(session: requests.Session, api_key: str, year: int, counties=ARC_COUNTIES) -> list[list[str]]:
    params = {
        "get": "NAME," + ",".join(ACS_VARIABLES),
        "for": "tract:*",
        "in": [f"state:{GEORGIA_FIPS}", "county:" + ",".join(sorted(counties))],
        "key": api_key,
    }
    try:
        resp = session.get(ACS_URL.format(year=year), params=params, timeout=120)
    except requests.RequestException as exc:  # exception text contains the URL (and the key)
        raise CensusApiError(f"Census API request failed: {type(exc).__name__}") from None
    if resp.status_code != 200 or not resp.headers.get("content-type", "").startswith("application/json"):
        raise CensusApiError(f"Census API returned HTTP {resp.status_code} (check the key / year {year})")
    return resp.json()


def _acs_value(raw: str | None) -> int | None:
    """ACS uses large negative sentinels (e.g. -666666666) for 'not available'; map them to NULL."""
    if raw in (None, ""):
        return None
    value = int(float(raw))
    return None if value < 0 else value


def acs_rows(table: list[list[str]], year: int) -> list[tuple]:
    """Convert the API's array-of-arrays (first row = header) into warehouse rows keyed by tract GEOID."""
    header, *records = table
    idx = {name: i for i, name in enumerate(header)}
    rows = []
    for rec in records:
        geoid = rec[idx["state"]] + rec[idx["county"]] + rec[idx["tract"]]
        rows.append((geoid, year, *(_acs_value(rec[idx[var]]) for var in ACS_VARIABLES)))
    return rows
