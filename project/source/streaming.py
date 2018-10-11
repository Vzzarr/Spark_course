from time import sleep

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

activity_data_path = "../../data/activity-data/"
static = spark.read.json(activity_data_path)
dataSchema = static.schema


streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json(activity_data_path)
activityCounts = streaming.groupBy("gt").count()

activityQuery = activityCounts.writeStream.queryName("activity_counts").format("memory").outputMode("complete").start()

spark.streams.active

for x in range(5):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(1)

activityQuery.awaitTermination()


#spark.readStream.format("socket").option("host", "localhost").option("port", "9999").option("includeTimestamp", "true").load()\
#  .writeStream.outputMode("append").format("console").option("truncate", "false").start().awaitTermination()
