#!/bin/bash
# Runs once, on first start of an empty Postgres volume.
# Creates the Airflow metadata DB and the analytics warehouse DB (with PostGIS).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" -v warehouse_pw="$WAREHOUSE_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;

CREATE ROLE warehouse LOGIN PASSWORD :'warehouse_pw';
CREATE DATABASE warehouse OWNER warehouse;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname warehouse <<'SQL'
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA raw AUTHORIZATION warehouse;
CREATE SCHEMA staging AUTHORIZATION warehouse;
CREATE SCHEMA marts AUTHORIZATION warehouse;
SQL
