
class StreamingSocket:
    def __init__(self, spark):
        self.spark = spark

    def run(self):
        # use "nc -lk 9999" for writing records for this example
        read_stream_socket = self.spark.readStream.format("socket").option("host", "localhost").option("port", "9999").\
            option("includeTimestamp", "true").load()

        read_stream_socket.writeStream.outputMode("update").format("console").option("truncate", "false").start().awaitTermination()

