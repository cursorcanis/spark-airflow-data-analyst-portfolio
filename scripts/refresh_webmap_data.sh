#!/usr/bin/env bash
# Snapshot the dbt marts into web/data/ for the public map (then commit + push; CI publishes GitHub Pages).
# The export runs ON THE SERVER inside the Airflow container (it can reach Postgres on the Docker network)
# and the files stream back over SSH as a tar archive, so no database port is needed on this machine.
#
# Usage: ATL_SERVER=user@host scripts/refresh_webmap_data.sh
set -euo pipefail
: "${ATL_SERVER:?set ATL_SERVER=user@host of the machine running the stack}"
REMOTE_DIR="${ATL_REMOTE_DIR:-~/spark-airflow-data-analyst-portfolio}"
cd "$(dirname "$0")/.."
mkdir -p web/data

ssh "$ATL_SERVER" "cd $REMOTE_DIR && docker compose exec -T airflow-scheduler bash -c '
  export PGHOST=\$DBT_PG_HOST PGUSER=\$DBT_PG_USER PGPASSWORD=\$DBT_PG_PASSWORD PGDATABASE=\$DBT_PG_DBNAME
  rm -rf /tmp/webmap && python -m atl_transit.webmap /tmp/webmap && tar -C /tmp/webmap -cf - .'" \
  | tar -C web/data -xf -

ls -l web/data
