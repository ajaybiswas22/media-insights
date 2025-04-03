from pyspark.sql import SparkSession
import threading
import time

class SparkManager:
    _instance = None
    _lock = threading.Lock()
    _last_used_time = time.time()

    @staticmethod
    def get_spark_session():
        """Get or create a shared Spark session."""
        with SparkManager._lock:
            if SparkManager._instance is None:
                print("Creating new Spark session...")
                SparkManager._instance = SparkSession.builder \
                    .appName("PersistentSparkCluster") \
                    .config("spark.driver.memory", "2g") \
                    .getOrCreate()
            else:
                print("Spark session exists")
                SparkManager._last_used_time = time.time()
            return SparkManager._instance

    @staticmethod
    def monitor_usage():
        """Monitor usage and stop Spark if inactive for 30 minutes."""
        while True:
            time.sleep(600)  # Check every 10 minutes
            with SparkManager._lock:
                if SparkManager._instance and time.time() - SparkManager._last_used_time > 1800:  # 30 mins
                    print("Stopping Spark session due to inactivity...")
                    SparkManager._instance.stop()
                    SparkManager._instance = None

# Start monitoring thread
threading.Thread(target=SparkManager.monitor_usage, daemon=True).start()
