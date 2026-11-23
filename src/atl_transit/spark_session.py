"""Spark Connect client session for Airflow tasks.

The task process only holds a thin gRPC client (`pyspark-client`); planning and execution happen on
the Spark Connect server (the driver) and the cluster's executors.
"""

from __future__ import annotations

import os


def get_spark():
    from pyspark.sql import SparkSession

    url = os.environ.get("SPARK_CONNECT_URL", "sc://spark-connect:15002")
    return SparkSession.builder.remote(url).getOrCreate()
