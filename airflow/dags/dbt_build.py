"""Build and test the dbt project (staging -> intermediate -> marts) in the Postgres warehouse.

Runs whenever any warehouse raw asset is updated (AssetAny via `|`). dbt lives in its own virtualenv
inside the Airflow image (/home/airflow/dbt-venv) so its dependencies can't clash with Airflow's.
`dbt build` = run every model and its tests in dependency order; a failing test stops downstream models.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from lake_assets import (
    DBT_MARTS,
    WAREHOUSE_CENSUS,
    WAREHOUSE_GTFS_STATIC,
    WAREHOUSE_RT_ARRIVALS,
    WAREHOUSE_RT_GPS,
)

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_ENV = {
    "DBT_PROFILES_DIR": "/opt/airflow/dbt",
    "DBT_PROJECT_DIR": "/opt/airflow/dbt",
    "DBT_TARGET_PATH": "/tmp/dbt/target",      # project dir is mounted read-only
    "DBT_LOG_PATH": "/tmp/dbt/logs",
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
}


@dag(
    dag_id="dbt_build",
    schedule=(WAREHOUSE_GTFS_STATIC | WAREHOUSE_RT_ARRIVALS | WAREHOUSE_RT_GPS | WAREHOUSE_CENSUS),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["dbt", "warehouse"],
    doc_md=__doc__,
)
def dbt_build():
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --target prod",
        env=DBT_ENV,
        append_env=True,
        outlets=[DBT_MARTS],
    )
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness --target prod",
        env=DBT_ENV,
        append_env=True,
        trigger_rule="all_done",   # report freshness even if a model failed
        retries=0,
    )
    build >> freshness


dbt_build()
