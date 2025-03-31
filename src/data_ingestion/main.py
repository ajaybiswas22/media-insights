import pandas as pd
from utils.youtube_client import YoutubeClient
from utils.vault_client import VaultClient
import json
import os
from dotenv import load_dotenv
load_dotenv()

hvac_client = VaultClient()
YOUTUBE_API_KEY = hvac_client.read_secret("secret/media_insights", "YOUTUBE_API_KEY")

minio_endpoint = os.getenv("MINIO_ENDPOINT","minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY","minio")
minio_secret_key = os.getenv("MINIO_SECRET_KEY","minio")
youtube = YoutubeClient(YOUTUBE_API_KEY)

response = youtube.search('snippet','monster energy drink',50)

print(json.dumps(response,indent=2))