from pyspark.sql import SparkSession

from project.streaming.socket_streaming import StreamingSocket
from project.streaming.streaming_test import StreamingTest

spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

activity_data_path = "../../data/activity-data/"
activity_data_schema = spark.read.json(activity_data_path).schema

StreamingTest(activity_data_path, activity_data_schema, spark).run()
StreamingSocket(spark).run()