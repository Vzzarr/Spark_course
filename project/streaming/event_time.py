
class StreamingEventTime:
    def __init__(self, data_path, data_schema, spark):
        self.data_path = data_path
        self.data_schema = data_schema
        self.spark = spark

    def run(self):
        streaming = self.spark.readStream.schema(self.data_schema)\
            .option("maxFilesPerTrigger", 10).json(self.data_path)

        withEventTime = streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

        withEventTime.printSchema()
