from time import sleep

import pyspark.sql.functions as f


class StreamingEventTime:
    def __init__(self, data_path, data_schema, spark):
        self.data_path = data_path
        self.data_schema = data_schema
        self.spark = spark

    def run(self):
        streaming = self.spark.readStream.schema(self.data_schema)\
            .option("maxFilesPerTrigger", 10).json(self.data_path)

        withEventTime = streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

        window_time = withEventTime.groupBy(f.window(f.col("event_time"), "10 minutes")).count()\
            .writeStream.queryName("events_per_window").format("memory").outputMode("complete").start()

        for x in range(5):
            self.spark.sql("SELECT * FROM events_per_window ORDER BY window").show(truncate=False)
            sleep(1)

        window_time.awaitTermination()