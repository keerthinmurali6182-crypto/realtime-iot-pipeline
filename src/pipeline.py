from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

class IoTStreamingPipeline:
    def __init__(self, spark, config):
        """
        Initializes the IoT Streaming Pipeline with a Spark Session and Configuration.
        """
        self.spark = spark
        self.config = config

    def get_iot_schema(self):
        """
        Defines the schema structure matching your incoming machinery telemetry logs.
        """
        return StructType([
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("status", StringType(), True)
        ])

    def start_stream(self):
        print("⚡ Spark Streaming Engine active. Listening to raw machinery data streams...")

        # 1. Read Raw Streaming Data from Local Host Socket (Port 9999)
        raw_socket_stream = self.spark.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 9999) \
            .load()

        # 2. Extract Data Schema Payload from JSON
        iot_schema = self.get_iot_schema()
        
        parsed_stream = raw_socket_stream \
            .selectExpr("CAST(value AS STRING) as raw_json_payload") \
            .select(from_json(col("raw_json_payload"), iot_schema).alias("data")) \
            .select("data.*")

        # 3. Apply Sliding Analytics Windows and Watermarking
        # Evaluates conditions over a 5-minute event-time window sliding every 2 minutes.
        # Drops arrival data older than 10 minutes automatically via watermarking.
        conformance_alerts = parsed_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes", "2 minutes"),
                col("device_id")
            ) \
            .count() \
            .select(
                col("window.start").alias("alert_window_start"),
                col("window.end").alias("alert_window_end"),
                col("device_id"),
                col("count").alias("anomaly_occurrences")
            )

        # 4. Stream Sink Destination Execution
        # Outputs streaming calculations directly to your terminal standard output
        query = conformance_alerts.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        # 5. Lock execution and bubble up reference to runner wrapper
        query.awaitTermination()
        return query