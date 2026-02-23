from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("local[*]") \
    .getOrCreate()

data = [("John", 30), ("Anna", 25)]

df = spark.createDataFrame(data, ["name", "age"])

df.show()

spark.stop()
