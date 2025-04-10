import sys
import glob
sys.path.append("/opt/bitnami/spark/python")
sys.path.append(glob.glob("/opt/bitnami/spark/python/lib/py4j-*-src.zip")[0])

from utils.spark_client import SparkSessionManager
from utils.vault_client import VaultClient
from utils.minio_client import MinioClient
from utils.file_client import InputOutputClient
from utils.boto3_client import Boto3Client
import os
import json
from dotenv import load_dotenv

def run_job(date: str) -> str:

    vault_address = os.getenv("VAULT_ADDR", "http://vault:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    hvac_client = VaultClient(vault_addr=vault_address,token=vault_token)
    MINIO_ACCESS_KEY = hvac_client.get_secret("media_insights", "MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = hvac_client.get_secret("media_insights", "MINIO_SECRET_KEY")
    minio_endpoint = os.getenv("MINIO_ENDPOINT","minio:9000")
    minio = MinioClient(minio_endpoint=minio_endpoint, minio_access_key=MINIO_ACCESS_KEY, minio_secret_key=MINIO_SECRET_KEY)
    boto = Boto3Client(minio)
    
    spark = SparkSessionManager.get_spark_session("media_insights","spark://spark:7077","1g","2")

    bucket_name = 'youtube'
    key = 'search_IN_news_today_2025-04-03.json'

    obj = boto.get_object(bucket_name, key)
    json_data = json.loads(obj['Body'].read())
    items = json_data.get('items',{})
    records = InputOutputClient.flatten_json(items)
    df = spark.createDataFrame(records)
    df.show()


    return "success"

if __name__ == "__main__":
    load_dotenv()
    if len(sys.argv) != 2:
        print("Error: Date missing")
        sys.exit(1)

    result = run_job(sys.argv[1])
    print(result)
