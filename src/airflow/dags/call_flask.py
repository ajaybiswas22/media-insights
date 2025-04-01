from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

with DAG(
    "call_flask_health_via_http",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    call_flask_health = SimpleHttpOperator(
        task_id="call_flask_health",
        method="GET",
        http_conn_id="flask_app",
        endpoint="health",
        response_check=lambda response: response.json()["status"] == "healthy",
        log_response=True,
    )

    call_flask_health