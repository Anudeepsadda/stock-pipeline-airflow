from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "stock_pipeline_dag",
    default_args=default_args,
    description="Fetch and store stock data daily",
    schedule_interval="@daily",  # can change to "@hourly"
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task: Run the fetch_stock_data.py script
    fetch_and_store = BashOperator(
        task_id="fetch_and_store_stock",
        bash_command="python /opt/airflow/scripts/fetch_stock_data.py",
    )

    fetch_and_store