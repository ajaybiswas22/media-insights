from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 2),
    "retries": 1,
}

with DAG(
    "youtube_search",
    default_args=default_args,
    schedule_interval="@daily",  # Runs daily, change as needed
    catchup=True,  # Allows DAG to run for missed execution dates
) as dag:

    run_youtube_api = BashOperator(
        task_id="youtube_ingest",
        bash_command="docker exec data_ingestion python /app/youtube_ingest.py {{ ds }}"
    )

    run_bronze_search = BashOperator(
        task_id="bronze_search",
        bash_command="docker exec data_engineering python /app/01_search.py"
    )

    run_youtube_api >> run_bronze_search
