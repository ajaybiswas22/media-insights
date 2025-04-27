import sys
import glob
import os
sys.path.append("/opt/bitnami/spark/python")
sys.path.append(glob.glob("/opt/bitnami/spark/python/lib/py4j-*-src.zip")[0])
from utils.spark_client import SparkSessionManager
from utils.vault_client import VaultClient
from utils.storage_client import MinioClient
from utils.ddl_client import DDLGenerator

def ddl_generate(yaml_path):

    vault_address = os.getenv("VAULT_ADDR", "http://vault:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    hvac_client = VaultClient(vault_addr=vault_address,token=vault_token)
    MINIO_ACCESS_KEY = hvac_client.get_secret("media_insights", "MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = hvac_client.get_secret("media_insights", "MINIO_SECRET_KEY")
    minio_endpoint = os.getenv("MINIO_ENDPOINT","minio:9000")
    minio = MinioClient(minio_endpoint=minio_endpoint, minio_access_key=MINIO_ACCESS_KEY, minio_secret_key=MINIO_SECRET_KEY)
    
    spark = SparkSessionManager.get_spark_session("media_insights","spark://spark:7077","1g","2",storage_client=minio)


    try:
        with open(yaml_path, "r") as f:
            yaml_string = f.read()
    except FileNotFoundError:
        return f"Error: {yaml_path} not found"
    except Exception as e:
        return f"Error reading {yaml_path}: {str(e)}"
    
    try:
        converter = DDLGenerator(yaml_content=yaml_string)
        database = "media_insights"
        all_queries = converter.generate_all_create_table_sql(database=database)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database};")
        for query in all_queries:
            spark.sql(query)

        return "DDL generation successful."
    
    except Exception as e:
        return f"Error during DDL generation: {str(e)}"

if __name__ == "__main__":
    print(ddl_generate('data_engineering/00_ddl.yaml'))