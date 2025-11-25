from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import json

DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    "BuildELT_Stock_Forecast_dbt",
    start_date=datetime(2025, 3, 19),
    description="dbt ELT for stock forecast analytics with ML forecasting",
    schedule=None,
    catchup=False,
) as dag:
    # Move this line inside the DAG
    conn = BaseHook.get_connection('snowflake_conn')

    # Read Airflow Variables for dbt configuration
    symbols_var = Variable.get("symbols", default_var=None)
    if symbols_var:
        try:
            symbols = json.loads(symbols_var)
        except Exception:
            symbols = [s.strip() for s in symbols_var.split(",") if s.strip()]
    else:
        symbols = ["AAPL", "NVDA"]
    
    train_lookback_days = Variable.get("train_lookback_days", default_var="180")
    forecast_horizon_days = Variable.get("forecast_horizon_days", default_var="14")
    forecast_prediction_interval = Variable.get("forecast_prediction_interval", default_var="0.95")

    # Build dbt vars JSON
    vars_json = json.dumps({
        "symbols": symbols,
        "train_lookback_days": int(train_lookback_days),
        "forecast_horizon_days": int(forecast_horizon_days),
        "forecast_prediction_interval": float(forecast_prediction_interval)
    })

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
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --vars '{vars_json}'",
        env=env_vars,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --vars '{vars_json}'",
        env=env_vars,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --vars '{vars_json}'",
        env=env_vars,
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"/home/airflow/.local/bin/dbt docs generate --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --vars '{vars_json}'",
        env=env_vars,
    )

    # Task flow: run -> test -> snapshot -> generate docs
    dbt_run >> dbt_test >> dbt_snapshot >> dbt_docs_generate
