# media-insights
Provides insights on social media content

## Running Locally

1. If poetry is not installed
```
pipx install poetry
```

2. Install dependencies
```
poetry config virtualenvs.create true
poetry lock
poetry install
source .venv/bin/activate
```

## Running using Docker

### Create self signed certificate for vault
```
cd src
```

### Run docker
```
docker-compose build --no-cache

docker exec -it vault vault operator init  # copy keys and token
docker exec -it vault vault operator unseal <Unseal_Key_1>
docker exec -it vault vault operator unseal <Unseal_Key_2>
docker exec -it vault vault operator unseal <Unseal_Key_3>
docker exec -it vault vault secrets enable -path=secret kv-v2
docker exec -it vault vault kv put secret/media_insights YOUTUBE_API_KEY="YOUR_YOUTUBE_API_KEY" MINIO_ACCESS_KEY="YOUR_MINIO_ACCESS_KEY" MINIO_SECRET_KEY="YOUR_MINIO_SECRET_KEY"
docker exec -it vault vault kv get secret/media_insights
docker exec -it vault vault policy write media_insights-policy /vault/policies/media_insights-policy.hcl
docker exec -it vault vault token create -policy=media_insights-policy  # copy token and paste in .env
docker-compose up  # you may need to unseal and restart containers
```

### Submit Spark Job
```
docker exec -it spark-worker /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties" \
/opt/bitnami/spark/data_engineering/01_search.py 2025-04-02
```

## Container List with ports

1. airflow webserver                8081->8080
2. bitnami/spark-worker             8083->8081
3. bitnami/spark-master             7077->7077, 8082->8080
4. bitnami/spark-history-server     18080->18080
5. bitnami/minio                    9000-9001->9000-9001
6. data_ingestion                   N/A
7. flask_app                        8000->8000
8. hashicorp/vault                  8200->8200
9. postgres:13                      5432/tcp
10. redis:latest                    6379/tcp
11. airflow init
12. airflow worker
13. airflow scheduler
14. airflow trigerrer


### Future diagrams
```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```