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
```
cd src
docker-compose build --no-cache
docker-compose up -d
```

```
docker exec -it vault vault kv put secret/data_ingestion YOUTUBE_API_KEY="YOUR_YOUTUBE_API_KEY"
```

```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```