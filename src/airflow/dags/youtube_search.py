from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 31),
    "retries": 1,
}

with DAG(
    "run_python_container",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or via API
    catchup=False,
) as dag:

    run_youtube_search = DockerOperator(
        task_id="youtube_search",
        image="data_ingestion",  # Replace with your actual image name
        api_version="auto",
        auto_remove=True,  # Deletes the container after execution
        command="python /app/main.py",  # Replace with your script path
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            "./app:/app",  # Mount code folder
            "./app/utils:/app/utils",  # Mount utilities
        ],
        tty=True,
    )

    run_youtube_search
