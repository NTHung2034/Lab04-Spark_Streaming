from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, avg, stddev,
    to_timestamp, when, isnan, isnull, coalesce, lit, collect_list
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


# Create SparkSession
spark = (SparkSession.builder
    .appName("SimpleMovingStats")
    .master("local[4]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
print("🚀 Starting Simple Moving Statistics Calculator...")

# Input schema
input_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("data_age_ms", IntegerType(), True)
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
    .withWatermark("event_time", "10 seconds")  # <- Watermark chỉ thêm tại đây
)

print("✅ Data stream prepared")

# Define windows - simplified approach
windows = [
    ("30s", "30 seconds"),
    ("1m", "1 minute"),
    ("5m", "5 minutes"),
    ("15m", "15 minutes"),
    ("30m", "30 minutes"),
    ("1h", "1 hour")
]

# Calculate stats for each window individually
window_dfs = []

for window_name, window_duration in windows:
    print(f"📊 Setting up {window_name} window...")
    
    # Calculate moving statistics for this window
    window_stats = (parsed_df
        .groupBy(
            window(col("event_time"), window_duration),
            col("symbol")
        )
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        )
        .select(
            col("window.end").alias("timestamp"),
            col("symbol"),
            lit(window_name).alias("window"),
            coalesce(col("avg_price"), lit(0.0)).alias("avg_price"),
            when(isnull(col("std_price")) | isnan(col("std_price")),
                 lit(0.0)).otherwise(col("std_price")).alias("std_price")
        )
    )
    
    window_dfs.append(window_stats)

# Union all windows
print("🔗 Combining all windows...")
all_stats = window_dfs[0]
for df in window_dfs[1:]:
    all_stats = all_stats.union(df)

# Group by timestamp and symbol for final output
final_output = (all_stats
    .groupBy("timestamp", "symbol")
    .agg(
        collect_list(
            struct("window", "avg_price", "std_price")
        ).alias("windows")
    )
    .select(
        col("timestamp").cast("string"),
        col("symbol"),
        col("windows")
    )
)

# Convert to JSON for Kafka
kafka_output = final_output.select(
    to_json(struct("timestamp", "symbol", "windows")).alias("value")
)

print("🚀 Starting output streams...")

# Write to Kafka with update mode
kafka_query = (kafka_output.writeStream
    .outputMode("update")  
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "btc-price-moving")
    .option("checkpointLocation", "/tmp/simple-moving-checkpoint")
    .trigger(processingTime='5 seconds')
    .start()
)

# Console output for monitoring
console_query = (final_output.writeStream
    .outputMode("update") 
    .format("console")
    .option("numRows", 5)
    .option("truncate", "false")
    .trigger(processingTime='10 seconds')
    .start()
)

print("✅ Moving statistics started.")
print("📊 Publishing to btc-price-moving topic")
print("🖥️  Console output every 10 seconds")
print("⏹️  Press Ctrl+C to stop...")

try:
    kafka_query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping moving statistics...")
    kafka_query.stop()
    console_query.stop()
    spark.stop()
    print("✅ Stopped.")
