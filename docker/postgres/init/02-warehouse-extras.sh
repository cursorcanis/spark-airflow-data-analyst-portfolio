#!/bin/bash
# Phase 4 warehouse setup: H3 extensions + a read-only `reporting` login for BI tools (Power BI).
# Idempotent: runs automatically on a fresh volume, and can be re-run on an existing one with
#   docker compose exec postgres bash /docker-entrypoint-initdb.d/02-warehouse-extras.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname warehouse \
  -v reporting_pw="${REPORTING_DB_PASSWORD:?REPORTING_DB_PASSWORD not set}" <<'SQL'
-- H3 is installed by a superuser (extensions run C code); then the warehouse role just calls its functions.
CREATE EXTENSION IF NOT EXISTS h3;
CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;

-- Create the role only if it doesn't exist yet (\gexec runs the generated statement).
SELECT 'CREATE ROLE reporting LOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reporting') \gexec
ALTER ROLE reporting WITH LOGIN PASSWORD :'reporting_pw';
-- Defence in depth: even if a grant slips, this session can't write; runaway queries are cut off.
ALTER ROLE reporting SET default_transaction_read_only = on;
ALTER ROLE reporting SET statement_timeout = '120s';

GRANT CONNECT ON DATABASE warehouse TO reporting;
GRANT USAGE ON SCHEMA marts TO reporting;          -- table-level SELECT comes from dbt `grants` config
REVOKE ALL ON SCHEMA raw, staging FROM reporting;
SQL
