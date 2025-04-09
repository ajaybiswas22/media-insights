# media-insights
Provides insights on social media content

# Running Locally

1. If poetry is not installed
```
brew install poetry
```
2. Create virtual environment
```
poetry config virtualenvs.create false
python3 -m venv mi-env
source mi-env/bin/activate
```
3. Install dependencies
```
poetry lock
poetry install
```

# Running using Docker

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
  /opt/bitnami/spark/data_engineering/01_search.py 2025-04-03
```

### Future diagrams
```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```