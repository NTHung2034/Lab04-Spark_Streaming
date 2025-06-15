from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, expr, explode,
    to_timestamp, when, isnan, isnull, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, ArrayType
)

# Create SparkSession
spark = (SparkSession.builder 
    .appName("BTCZScoreCalculator") 
    .master("local[4]") 
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") 
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

# Define schema for btc-price topic
price_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("data_age_ms", IntegerType(), True)
])

# Define schema for btc-price-moving topic
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

print("Starting Z-Score Calculator...")
print("Subscribing to btc-price and btc-price-moving topics...")

# Read from btc-price topic
price_stream = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "localhost:9092") 
    .option("subscribe", "btc-price") 
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(
        from_json(col("value").cast("string"), price_schema).alias("price_data")
    )
    .select("price_data.*")
    .withColumn("price_timestamp", to_timestamp(col("event_time")))
    .filter(col("price_timestamp").isNotNull()) 
    .withColumnRenamed("event_time", "price_event_time")
)

# Read from btc-price-moving topic  
moving_stream = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "localhost:9092") 
    .option("subscribe", "btc-price-moving") 
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(
        from_json(col("value").cast("string"), moving_schema).alias("moving_data")
    )
    .select("moving_data.*")
    .withColumn("moving_timestamp", to_timestamp(col("timestamp")))
    .filter(col("moving_timestamp").isNotNull()) 
    .withColumnRenamed("timestamp", "moving_event_time")
)

print("Price stream schema:")
price_stream.printSchema()
print("Moving statistics stream schema:")
moving_stream.printSchema()

# Apply watermark
price_df = price_stream.withWatermark("price_timestamp", "10 seconds").alias("price_df")
moving_df = moving_stream.withWatermark("moving_timestamp", "10 seconds").alias("moving_df")

# Perform stream-to-stream join
joined_stream = (price_df
    .join(
        moving_df,
        (col("price_df.price_timestamp") == col("moving_df.moving_timestamp")) & 
        (col("price_df.symbol") == col("moving_df.symbol")),
        "inner"
    )
    .select(
        col("price_df.price_timestamp").alias("timestamp"),
        col("price_df.symbol").alias("symbol"),
        col("price_df.price").alias("price"),
        col("moving_df.windows").alias("windows")
    )
)

print("Joined stream schema:")
joined_stream.printSchema()

# Explode the windows array to calculate Z-score for each window
exploded_stream = (joined_stream
    .select(
        col("timestamp"),
        col("symbol"),
        col("price"),
        explode(col("windows")).alias("window_stats")
    )
    .select(
        col("timestamp"),
        col("symbol"),
        col("price"),
        col("window_stats.window").alias("window"),
        col("window_stats.avg_price").alias("avg_price"),
        col("window_stats.std_price").alias("std_price")
    )
)

# Calculate Z-score: (price - avg) / std
zscore_stream = exploded_stream.withColumn(
    "zscore_price",
    when(
        (col("std_price") == 0) | isnull(col("std_price")) | isnan(col("std_price")),
        lit(0.0)
    ).otherwise(
        (col("price") - col("avg_price")) / col("std_price")
    )
).select(
    col("timestamp"),
    col("symbol"),
    col("window"),
    col("zscore_price")
)

# Group back by timestamp and symbol to create the required JSON format
final_zscores = (zscore_stream
    .withWatermark("timestamp", "10 seconds")
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

# Convert to JSON format for Kafka
kafka_output = final_zscores.select(
    to_json(
        struct(
            col("timestamp"),
            col("symbol"),
            col("zscores")
        )
    ).alias("value")
)

print("Starting streaming query to btc-price-zscore topic...")

# Write to Kafka btc-price-zscore topic
query = (kafka_output.writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "btc-price-zscore")
    .option("checkpointLocation", "/tmp/kafka-zscore-checkpoint")
    .trigger(processingTime='1 seconds')
    .start()
)

# Also write to console for debugging
console_query = (final_zscores.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime='5 seconds')
    .start()
)

print("Z-Score calculation started!")
print("Publishing to btc-price-zscore topic...")
print("Press Ctrl+C to stop...")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Z-Score calculator...")
    query.stop()
    console_query.stop()
    spark.stop()
