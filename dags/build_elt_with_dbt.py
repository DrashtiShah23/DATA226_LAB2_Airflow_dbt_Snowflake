from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    "BuildELT_Stock_Forecast_dbt",
    start_date=datetime(2025, 3, 19),
    description="dbt ELT for stock forecast analytics",
    schedule=None,
    catchup=False,
) as dag:
    # Move this line inside the DAG
    conn = BaseHook.get_connection('snowflake_conn')

    env_vars = {
        "DBT_USER": conn.login,
        "DBT_PASSWORD": conn.password,
        "DBT_ACCOUNT": conn.extra_dejson.get("account"),
        "DBT_SCHEMA": conn.schema,
        "DBT_DATABASE": conn.extra_dejson.get("database"),
        "DBT_ROLE": conn.extra_dejson.get("role"),
        "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
        "DBT_TYPE": "snowflake",
    }

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=env_vars,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=env_vars,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=env_vars,
    )

    dbt_run >> dbt_test >> dbt_snapshot
