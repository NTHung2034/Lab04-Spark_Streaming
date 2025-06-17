from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, explode, 
    to_timestamp, when, isnan, isnull, coalesce, lit,
    expr, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

# Create SparkSession
spark = (SparkSession.builder
    .appName("ZScoreCalculator")
    .master("local[4]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
# Disable watermark correctness check for stream-stream joins
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
print("Starting Z-Score Calculator...")

# Schema for btc-price topic (original price data)
price_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True)
])

# Schema for btc-price-moving topic (moving statistics)
window_stats_schema = StructType([
    StructField("window", StringType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("std_price", DoubleType(), True)
])

moving_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("windows", ArrayType(window_stats_schema), True)
])

print("Setting up Kafka streams...")

# Read from btc-price topic (original price data)
price_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "btc-price")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), price_schema).alias("price_data"))
    .select("price_data.*")
    .withColumn("event_time", to_timestamp(col("event_time")))
    .filter(col("event_time").isNotNull())
    .withWatermark("event_time", "10 seconds")
)

print("Price stream configured")

# Read from btc-price-moving topic (moving statistics)
moving_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "btc-price-moving")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), moving_schema).alias("moving_data"))
    .select("moving_data.*")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .filter(col("timestamp").isNotNull())
    .withWatermark("timestamp", "10 seconds")
)

print("Moving statistics stream configured")

# Flatten the moving statistics to have one row per window
flattened_moving = (moving_stream
    .select(
        col("timestamp"),
        col("symbol"),
        explode(col("windows")).alias("window_data")
    )
    .select(
        col("timestamp"),
        col("symbol"),
        col("window_data.window").alias("window"),
        col("window_data.avg_price").alias("avg_price"),
        col("window_data.std_price").alias("std_price")
    )
)

print("Performing stream-stream join...")

# Join price stream with moving statistics stream
# Join condition: same timestamp and symbol
joined_stream = (price_stream.alias("p")
    .join(
        flattened_moving.alias("m"),
        (col("p.event_time") == col("m.timestamp")) & 
        (col("p.symbol") == col("m.symbol")),
        "inner"
    )
    .select(
        col("p.event_time").alias("timestamp"),
        col("p.symbol"),
        col("p.price"),
        col("m.window"),
        col("m.avg_price"),
        col("m.std_price")
    )
)

print("Calculating Z-scores...")

# Calculate Z-scores for each window
# Z-score = (price - mean) / std_dev
zscore_stream = (joined_stream
    .withColumn(
        "zscore_price",
        when(
            (col("std_price") == 0) | isnull(col("std_price")) | isnan(col("std_price")),
            lit(0.0)  # If std is 0 or null, set zscore to 0
        ).otherwise(
            (col("price") - col("avg_price")) / col("std_price")
        )
    )
    .select(
        col("timestamp"),
        col("symbol"),
        col("window"),
        col("zscore_price")
    )
)

print("Grouping Z-scores by timestamp...")

# Group Z-scores by timestamp and symbol to create final output format
final_zscores = (zscore_stream
    .groupBy("timestamp", "symbol")
    .agg(
        expr("collect_list(struct(window, zscore_price))").alias("zscores")
    )
    .select(
        col("timestamp").cast("string").alias("timestamp"),
        col("symbol"),
        col("zscores")
    )
)

# Convert to JSON for Kafka output
kafka_output = final_zscores.select(
    to_json(struct("timestamp", "symbol", "zscores")).alias("value")
)

print("Starting output streams...")

# Write to Kafka topic
kafka_query = (kafka_output.writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "btc-price-zscore")
    .option("checkpointLocation", "./tmp/zscore-checkpoint")
    .trigger(processingTime='5 seconds')
    .start()
)

# Console output for monitoring
console_query = (final_zscores.writeStream
    .outputMode("append")
    .format("console")
    .option("numRows", 3)
    .option("truncate", "false")
    .trigger(processingTime='10 seconds')
    .start()
)

print("Z-Score calculator started")
print("Publishing to btc-price-zscore topic")
print("Console output every 10 seconds")
print("Joining btc-price and btc-price-moving streams")
print("Calculating Z-scores for all windows: 30s, 1m, 5m, 15m, 30m, 1h")
print("Press Ctrl+C to stop...")

try:
    kafka_query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Z-Score calculator...")
    kafka_query.stop()
    console_query.stop()
    spark.stop()
    print("Stopped successfully.")
