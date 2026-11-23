"""Shared Airflow Asset definitions (data-aware scheduling).

A producer task declares `outlets=[ASSET]`; when it succeeds, Airflow records an asset event and
every DAG with `schedule=[ASSET]` is triggered. DAGs are chained by the data they produce,
not by guessing clock times.
"""

from airflow.sdk import Asset

RAW_GTFS_STATIC = Asset("s3://raw/gtfs_static")
RAW_RT_PARQUET = Asset("s3://raw/gtfs_rt_parquet")
CURATED_GTFS_STATIC = Asset("s3://curated/gtfs_static")
CURATED_VEHICLE_POSITIONS = Asset("s3://curated/vehicle_positions")
CURATED_STOP_ARRIVALS = Asset("s3://curated/stop_arrivals")

# Warehouse (Postgres) assets: published raw tables that dbt builds on.
WAREHOUSE_GTFS_STATIC = Asset(name="warehouse_raw_gtfs_static",
                              uri="postgres://postgres:5432/warehouse/raw/gtfs_stop_times")
WAREHOUSE_RT_ARRIVALS = Asset(name="warehouse_raw_rt_stop_arrivals",
                              uri="postgres://postgres:5432/warehouse/raw/rt_stop_arrivals")
WAREHOUSE_RT_GPS = Asset(name="warehouse_raw_rt_gps",
                         uri="postgres://postgres:5432/warehouse/raw/rt_vehicle_pings")
WAREHOUSE_CENSUS = Asset(name="warehouse_raw_census",
                         uri="postgres://postgres:5432/warehouse/raw/acs_tract_demographics")
DBT_MARTS = Asset(name="dbt_marts", uri="postgres://postgres:5432/warehouse/marts/fct_stop_arrival")
