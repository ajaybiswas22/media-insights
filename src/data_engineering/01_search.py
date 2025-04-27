import sys
import glob
sys.path.append("/opt/bitnami/spark/python")
sys.path.append(glob.glob("/opt/bitnami/spark/python/lib/py4j-*-src.zip")[0])

from utils.spark_client import SparkSessionManager
from utils.vault_client import VaultClient
from utils.storage_client import MinioClient
from utils.file_client import InputOutputClient
from utils.boto3_client import Boto3Client
import os
import json
from dotenv import load_dotenv
from functools import reduce
import pyspark.sql.functions as F
from delta.tables import DeltaTable

def run_job(date: str = None) -> str:

    vault_address = os.getenv("VAULT_ADDR", "http://vault:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    hvac_client = VaultClient(vault_addr=vault_address,token=vault_token)
    MINIO_ACCESS_KEY = hvac_client.get_secret("media_insights", "MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = hvac_client.get_secret("media_insights", "MINIO_SECRET_KEY")
    minio_endpoint = os.getenv("MINIO_ENDPOINT","minio:9000")
    minio = MinioClient(minio_endpoint=minio_endpoint, minio_access_key=MINIO_ACCESS_KEY, minio_secret_key=MINIO_SECRET_KEY)
    boto = Boto3Client(minio)
    
    spark = SparkSessionManager.get_spark_session("media_insights","spark://spark:7077","1g","2",storage_client=minio)
    hadoop_version = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    print(f"Hadoop version: {hadoop_version}")

    bucket_name = 'youtube'
    file_list = minio.list_files(bucket=bucket_name)
    print(file_list)

    df_list = []
    for key in file_list:
        if date is None or (date in key):
            obj = boto.get_object(bucket_name, key)
            json_data = json.loads(obj['Body'].read())
            items = json_data.get('items',{})
            records = InputOutputClient.flatten_json(items)
            df = spark.createDataFrame(records)
            df = df.withColumn('country',F.lit(key.split('_')[1]))
            df = df.withColumn('date',F.to_date(F.lit(date),"yyyy-MM-dd"))
            df = df.withColumn('file_name',F.lit(key))
            df = df.withColumn('file_name',F.lit(key))
            df = df.withColumn('load_timestamp',F.current_timestamp())
            df_list.append(df)
    
    if df_list == []:
        return "no files"

    df_combined = reduce(lambda df1, df2: df1.unionByName(df2), df_list)
    df_combined.groupBy('country').count().show()
    target_bucket = 'delta-store'
    df_combined.write.format("delta").mode("append").save(f"s3a://{target_bucket}/bronze_search_raw")

    return "success"

if __name__ == "__main__":
    load_dotenv()
    if len(sys.argv) >= 2:
        result = run_job(sys.argv[1])
    elif len(sys.argv) == 1:
        result = run_job()
    else:
        result = None
    print(result)
