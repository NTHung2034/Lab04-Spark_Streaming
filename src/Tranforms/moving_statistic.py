from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, avg, stddev,
    to_timestamp, when, isnan, isnull, coalesce, lit, collect_list
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create SparkSession
spark = (SparkSession.builder
    .appName("MovingStatsCalculator")
    .master("local[4]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
# Disable the watermark correctness check to handle sliding windows
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
print("Starting Moving Statistics Calculator...")

# Correct input schema based on requirements
input_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True)  # ISO8601 timestamp from extract stage
])

# Read from Kafka
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "btc-price")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse and prepare data
parsed_df = (df
    .select(from_json(col("value").cast("string"), input_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("event_time")))
    .filter(col("event_time").isNotNull()) 
    .withWatermark("event_time", "10 seconds")
)

print("Data stream prepared")

# Define sliding windows - using same slide interval to avoid watermark conflicts
# Using consistent slide duration for all windows
slide_interval = "10 seconds"
windows_config = [
    ("30s", "30 seconds", slide_interval),
    ("1m", "1 minute", slide_interval),
    ("5m", "5 minutes", slide_interval),
    ("15m", "15 minutes", slide_interval),
    ("30m", "30 minutes", slide_interval),
    ("1h", "1 hour", slide_interval)
]

# Calculate stats for each sliding window
window_dfs = []

for window_name, window_duration, slide_duration in windows_config:
    print(f"Setting up sliding window: {window_name}")
    
    window_stats = (parsed_df
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("symbol")
        )
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        )
        .select(
            # Use window end time as the timestamp for this window
            col("window.end").alias("window_timestamp"),
            col("symbol"),
            lit(window_name).alias("window"),
            coalesce(col("avg_price"), lit(0.0)).alias("avg_price"),
            # Handle null/NaN standard deviation (when only 1 data point)
            when(isnull(col("std_price")) | isnan(col("std_price")),
                 lit(0.0)).otherwise(col("std_price")).alias("std_price")
        )
    )
    
    window_dfs.append(window_stats)

# Union all window calculations
print("Combining all sliding windows...")
all_windows = window_dfs[0]
for df in window_dfs[1:]:
    all_windows = all_windows.union(df)

# Group by timestamp and symbol to create final output format
final_output = (all_windows
    .groupBy("window_timestamp", "symbol")
    .agg(
        collect_list(
            struct("window", "avg_price", "std_price")
        ).alias("windows")
    )
    .select(
        # Convert timestamp back to ISO8601 string format
        col("window_timestamp").cast("string").alias("timestamp"),
        col("symbol"),
        col("windows")
    )
)

# Convert to JSON for Kafka output
kafka_output = final_output.select(
    to_json(struct("timestamp", "symbol", "windows")).alias("value")
)

print("Starting output streams...")

# Write to Kafka topic
kafka_query = (kafka_output.writeStream
    .outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "btc-price-moving")
    .option("checkpointLocation", "./tmp/moving-stats-checkpoint")
    .trigger(processingTime='5 seconds')
    .start()
)

# Console output for monitoring
console_query = (final_output.writeStream
    .outputMode("update")
    .format("console")
    .option("numRows", 3)
    .option("truncate", "false")
    .trigger(processingTime='10 seconds')
    .start()
)

print("Moving statistics calculator started")
print("Publishing to btc-price-moving topic")
print("Console output every 10 seconds")
print("Calculating sliding windows: 30s, 1m, 5m, 15m, 30m, 1h")
print("Press Ctrl+C to stop...")

try:
    kafka_query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping moving statistics calculator...")
    kafka_query.stop()
    console_query.stop()
    spark.stop()
    print("Stopped successfully.")
 
