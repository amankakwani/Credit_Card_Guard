from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Initialize Spark (The Engine)
# We tell it to use the "Kafka Package" so it understands the stream
spark = SparkSession.builder \
    .appName("FraudDetector") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# 2. Define the Schema (The Blueprint)
# This must match what we wrote in producer.py
schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("location", StringType()),
    StructField("home_loc", StringType()), # Added this field
    StructField("timestamp", DoubleType()),
    StructField("is_fraud", LongType())
])

# 3. Read from Kafka (The Input)
# We subscribe to the 'transactions' topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .load()

# 4. Parse the Data
# Kafka sends data as bytes. We cast it to String, then Parse JSON.
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Simple Logic (For now)
# Just filter for High Amounts (> 1000) to prove it works
fraud_df = parsed_df.filter(col("amount") > 1000)
# fraud_df = parsed_df # Show EVERYTHING to prove it works

# 6. Write to Console (The Output)
# This prints the results to your terminal in real-time
query_console = fraud_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

# 7. Write to MinIO (The Storage)
# We save ALL data (parsed_df) so we can train models later
query_minio = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://transactions/data/") \
    .option("checkpointLocation", "s3a://transactions/checkpoints/") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Spark Streaming is running...")
spark.streams.awaitAnyTermination()
