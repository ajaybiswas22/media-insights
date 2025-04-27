from pyspark.sql import SparkSession

class SparkSessionManager:

    @staticmethod
    def get_spark_session(app_name: str, master: str = "spark://spark:7077", executor_memory: str = "1g", executor_cores: str = "2", storage_client: object = None):
        
        if storage_client:
            return SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.executor.cores", executor_cores) \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "file:/opt/bitnami/spark/spark-events") \
            .config("spark.hadoop.fs.s3a.access.key", storage_client.get_access_key()) \
            .config("spark.hadoop.fs.s3a.secret.key", storage_client.get_secret_key()) \
            .config("spark.hadoop.fs.s3a.endpoint", storage_client.get_endpoint()) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        else:
            return SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.executor.cores", executor_cores) \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "file:/opt/bitnami/spark/spark-events") \
            .getOrCreate()
