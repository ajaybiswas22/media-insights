import pandas as pd
from utils.youtube_client import YoutubeClient
from utils.vault_client import VaultClient
from utils.minio_client import MinioClient
import json
import os
from dotenv import load_dotenv
load_dotenv()

vault_address = os.getenv("VAULT_ADDR", "http://vault:8200")
vault_token = os.getenv("VAULT_TOKEN")
hvac_client = VaultClient(vault_addr=vault_address,token=vault_token)
YOUTUBE_API_KEY = hvac_client.get_secret("media_insights", "YOUTUBE_API_KEY")

minio_endpoint = os.getenv("MINIO_ENDPOINT","minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY","minio")
minio_secret_key = os.getenv("MINIO_SECRET_KEY","minio")
youtube = YoutubeClient(YOUTUBE_API_KEY)

response = youtube.search('snippet','monster energy drink',1)

minio = MinioClient(minio_endpoint=minio_endpoint, minio_access_key=minio_access_key, minio_secret_key=minio_secret_key)
minio.upload_json("youtube", "response.json", response, overwrite=True)
response_json = minio.download_json("youtube", "response.json")

print(response_json)