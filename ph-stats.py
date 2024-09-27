
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg

# Create Spark session
spark = SparkSession.builder \
    .appName("WaterQualityStreaming") \
    .getOrCreate()

# Set Kafka parameters
kafka_bootstrap_servers = "localhost:9092"  # Adjust based on your Kafka server
input_topic = "water-quality-data"
output_topic = "avg-ph-by-sensor-5-min"

# Create DataFrame from Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .load()

# Deserialize the JSON data
water_quality_data = df.selectExpr("CAST(value AS STRING) as json") \
    .selectExpr("from_json(json, 'sensor_id STRING, timestamp DOUBLE, pH DOUBLE') as data") \
    .select("data.*")

# Calculate rolling average of pH by sensor ID with a 5-minute window
rolling_avg_pH = water_quality_data \
    .groupBy(
        window(col("timestamp"), "5 minutes"), 
        col("sensor_id")
    ) \
    .agg(avg("pH").alias("avg_pH"))

# Write the results to Kafka
query = rolling_avg_pH \
    .selectExpr("CAST(sensor_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", output_topic) \
    .start()

# Start the streaming query
query.awaitTermination()
