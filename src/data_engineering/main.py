from utils.shared_spark import SparkManager
import time

if __name__ == "__main__":
    spark = SparkManager.get_spark_session()
    print("Spark session initialized.")
    
    while True:
        time.sleep(60)  # Keep alive
