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
