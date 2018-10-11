from time import sleep


class StreamingTest:
    def __init__(self, data_path, data_schema, spark):
        self.data_path = data_path
        self.data_schema = data_schema
        self.spark = spark

    def run(self):
        streaming = self.spark.readStream.schema(self.data_schema).option("maxFilesPerTrigger", 1).json(self.data_path)
        activity_counts = streaming.groupBy("gt").count()

        activity_query = activity_counts.writeStream.queryName("activity_counts").format("memory").outputMode(
            "complete").start()

        for x in range(5):
            self.spark.sql("SELECT * FROM activity_counts").show()
            sleep(1)

        activity_query.awaitTermination()
