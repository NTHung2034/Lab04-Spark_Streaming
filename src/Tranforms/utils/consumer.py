from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create SparkSession
spark = (SparkSession.builder 
    .appName("KafkaReadWrite") 
    .master("local[4]") 
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") 
    .getOrCreate()
)

# Define schema
schema = StructType([
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
    .option("startingOffsets", "earliest") 
    .load()
)

# Parse Kafka data
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*").withColumn("event_time", to_timestamp("event_time"))


# Print schema
print("\n\nParsed DataFrame Schema:\n\n")
parsed_df.printSchema()

# Write to console for streaming
print("\n\nStarting streaming query...\n\n")
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()