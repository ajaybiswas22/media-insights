from pyspark.sql import SparkSession

class SparkSessionManager:

    @staticmethod
    def get_spark_session(app_name: str, master: str = "spark://spark:7077", executor_memory: str = "1g", executor_cores: str = "2"):
        return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:/opt/bitnami/spark/spark-events") \
        .getOrCreate()