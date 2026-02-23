from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CreateData") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "John", 30),
    (2, "Anna", 25),
    (3, "Mike", 40)
]

df = spark.createDataFrame(data, ["id", "name", "age"])

# write parquet file
df.write.mode("overwrite").parquet(
    "/Users/subinssubins/data_engineering/data/customers.parquet"
)

spark.stop()
