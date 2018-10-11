from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

activity_data_path = "../../data/activity-data/"
static = spark.read.json(activity_data_path)
dataSchema = static.schema

# use "nc -lk 9999" for writing records for this example

spark.readStream.format("socket").option("host", "localhost").option("port", "9999").option("includeTimestamp", "true").load()\
  .writeStream.outputMode("append").format("console").option("truncate", "false").start().awaitTermination()

