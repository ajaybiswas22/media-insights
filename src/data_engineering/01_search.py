from utils.shared_spark import SparkManager

spark = SparkManager.get_spark_session()
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()