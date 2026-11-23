# Airflow image + the extra Python packages our DAGs need.
# Extending the official image (instead of _PIP_ADDITIONAL_REQUIREMENTS at startup) makes
# builds reproducible and container starts fast.
FROM apache/airflow:3.4.0-python3.12

# Pin apache-airflow so pip can't up/downgrade it; protobuf<7 matches the image's own version.
# pyspark-client = thin Spark Connect client (no JVM); zstandard is one of its runtime deps.
RUN pip install --no-cache-dir "apache-airflow==3.4.0" "gtfs-realtime-bindings==2.0.0" "protobuf>=6,<7" \
    "pyspark-client==4.2.1" "zstandard>=0.25"

# dbt in its own virtualenv: dbt-core pins its own versions of jinja/click/protobuf etc., so keeping it
# out of Airflow's environment avoids dependency conflicts. Called as /home/airflow/dbt-venv/bin/dbt.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.7" "dbt-postgres==1.11.0"
