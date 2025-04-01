from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 31),
    "retries": 1,
}

with DAG(
    "youtube_search",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or via API
    catchup=False,
) as dag:

    run_youtube_api = BashOperator(
        task_id="youtube_ingest",
        bash_command="docker exec data_ingestion python /app/youtube_ingest.py 2025-04-01"
    )

    run_youtube_api
