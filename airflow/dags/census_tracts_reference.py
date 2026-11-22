"""Reference data: Census tract boundaries for the 11-county Atlanta region -> lake + PostGIS.

fetch (TIGERweb, paged GeoJSON) -> verify count -> land raw GeoJSON in the lake ->
full refresh of warehouse table raw.census_tracts (truncate + insert in ONE transaction,
so readers never see a half-loaded table) with a GiST spatial index.
Tract boundaries change once per decade, so a quarterly refresh is plenty.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from atl_transit.census import count_tracts, feature_to_row, fetch_tracts
from atl_transit.gtfs_rt import USER_AGENT

LAKE_CONN_ID = "lake_s3"
WAREHOUSE_CONN_ID = "warehouse_pg"
RAW_BUCKET = "raw"

DDL = """
CREATE TABLE IF NOT EXISTS raw.census_tracts (
    geoid        text PRIMARY KEY,
    state_fips   text NOT NULL,
    county_fips  text NOT NULL,
    county_name  text,
    tract_code   text NOT NULL,
    tract_name   text,
    aland_m2     bigint,
    awater_m2    bigint,
    intpt_lat    double precision,
    intpt_lon    double precision,
    geom         geometry(MultiPolygon, 4326) NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS census_tracts_geom_gix ON raw.census_tracts USING gist (geom);
"""

# ST_Multi: some tracts are Polygon, some MultiPolygon -> store one consistent type.
INSERT = """
INSERT INTO raw.census_tracts
    (geoid, state_fips, county_fips, county_name, tract_code, tract_name,
     aland_m2, awater_m2, intpt_lat, intpt_lon, geom)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))
"""


@dag(
    dag_id="census_tracts_reference",
    schedule="0 7 1 */3 *",  # 07:00 on the 1st of every 3rd month
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["reference", "census", "postgis"],
    doc_md=__doc__,
)
def census_tracts_reference():
    @task
    def extract_to_lake() -> dict:
        session = requests.Session()
        session.headers["User-Agent"] = USER_AGENT
        expected = count_tracts(session)
        features = list(fetch_tracts(session))
        if len(features) != expected or len({f["properties"]["GEOID"] for f in features}) != expected:
            raise ValueError(f"expected {expected} unique tracts, got {len(features)}")

        key = f"census_tracts/ingest_date={datetime.now(UTC):%Y-%m-%d}/tracts_arc_region.geojson"
        S3Hook(aws_conn_id=LAKE_CONN_ID).load_string(
            json.dumps({"type": "FeatureCollection", "features": features}), key, RAW_BUCKET, replace=True
        )
        return {"key": key, "tracts": expected}

    @task
    def load_to_postgis(meta: dict) -> int:
        body = S3Hook(aws_conn_id=LAKE_CONN_ID).read_key(meta["key"], RAW_BUCKET)
        rows = [feature_to_row(f) for f in json.loads(body)["features"]]

        conn = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()
        try:
            # Everything below is ONE transaction: either the full new table is committed, or nothing changes.
            with conn.cursor() as cur:
                cur.execute(DDL)
                cur.execute("TRUNCATE raw.census_tracts")
                cur.executemany(INSERT, rows)
                cur.execute("SELECT count(*), count(*) FILTER (WHERE NOT ST_IsValid(geom)) FROM raw.census_tracts")
                loaded, invalid = cur.fetchone()
                if loaded != meta["tracts"]:
                    raise ValueError(f"loaded {loaded} rows, expected {meta['tracts']}")
                cur.execute("ANALYZE raw.census_tracts")  # refresh planner statistics
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        print(f"loaded={loaded} invalid_geometries={invalid}")
        return loaded

    load_to_postgis(extract_to_lake())


census_tracts_reference()
