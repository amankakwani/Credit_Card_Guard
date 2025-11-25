from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lag, avg, unix_timestamp, from_unixtime, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
import mlflow
import mlflow.spark

# DATA SOURCE CONFIGURATION
# Change this to match your data source: 'synthetic', 'bank_a', 'bank_b', 'payment_gateway'
DATA_SOURCE = "synthetic"

# SCHEMA MAPPINGS for different data sources
SCHEMA_MAPPINGS = {
    "synthetic": {
        "user_id": "user_id",
        "amount": "amount",
        "location": "location",
        "timestamp": "timestamp",
        "home_loc": "home_loc",
        "is_fraud": "is_fraud"
    },
    "bank_a": {
        "user_id": "customer_number",
        "amount": "transaction_amount_usd",
        "location": "merchant_country",
        "timestamp": "event_time",
        "home_loc": "billing_address_country"
    }
}

print(f"🔧 Configured for data source: {DATA_SOURCE}")

# 1. Initialize Spark with all required packages
spark = SparkSession.builder \
    .appName("FraudDetectorRealTime") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# 2. Define schema based on data source
# For simplicity, we use a generic schema that covers all sources
input_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("customer_number", StringType(), True),  # Bank A
    StructField("amount", DoubleType(), True),
    StructField("transaction_amount_usd", DoubleType(), True),  # Bank A
    StructField("location", StringType(), True),
    StructField("merchant_country", StringType(), True),  # Bank A
    StructField("timestamp", DoubleType(), True),
    StructField("event_time", DoubleType(), True),  # Bank A
    StructField("home_loc", StringType(), True),
    StructField("billing_address_country", StringType(), True),  # Bank A
    StructField("is_fraud", LongType(), True)  # Optional (only in training)
])

# 3. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .load()

# 4. Parse JSON data
parsed_df = df.select(
    from_json(col("value").cast("string"), input_schema).alias("data")
).select("data.*")

# 5. SCHEMA MAPPING - Transform to standard format
mapping = SCHEMA_MAPPINGS[DATA_SOURCE]

# Map fields from source schema to our standard schema
standardized_df = parsed_df.select(
    col(mapping["user_id"]).alias("user_id"),
    col(mapping["amount"]).alias("amount"),
    col(mapping["location"]).alias("location"),
    col(mapping["timestamp"]).alias("timestamp"),
    col(mapping["home_loc"]).alias("home_loc")
)

# 6. SIMPLIFIED FEATURE ENGINEERING (Streaming-compatible)
print("🧠 Engineering features...")

# A. Time-based features
standardized_df = standardized_df.withColumn("datetime", from_unixtime("timestamp"))
standardized_df = standardized_df.withColumn("hour", hour("datetime"))

# B. Foreign transaction detection
standardized_df = standardized_df.withColumn("is_foreign", (col("location") != col("home_loc")).cast("int"))

# C. Simple derived features (no windowing)
standardized_df = standardized_df.withColumn("time_diff", col("timestamp") * 0)  # Placeholder (always 0)
standardized_df = standardized_df.withColumn("amount_ratio", col("amount") / 100.0)  # Normalized amount

# 7. PREPARE FEATURES FOR MODEL
feature_cols = ["amount", "hour", "time_diff", "amount_ratio", "is_foreign"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
feature_df = assembler.transform(standardized_df)

# 8. LOAD MODEL FROM MLFLOW
print("🤖 Loading ML model from MLflow...")
mlflow.set_tracking_uri("http://mlflow:5000")

# Get the latest production model (or specify run_id)
# For now, we'll use a specific run - you can get this from MLflow UI
# TODO: Replace with your actual run_id from MLflow
# Or use: model = mlflow.spark.load_model("models:/fraud_model/Production")

# For demonstration, we'll use the model directly by fetching latest run
from mlflow.tracking import MlflowClient
client = MlflowClient()
experiment = client.get_experiment_by_name("fraud_detection_experiment")
runs = client.search_runs(experiment.experiment_id, order_by=["start_time DESC"], max_results=1)

if len(runs) > 0:
    run_id = runs[0].info.run_id
    model_uri = f"runs:/{run_id}/fraud_model"
    print(f"📦 Loading model from run: {run_id}")
    
    try:
        model = mlflow.spark.load_model(model_uri)
        print("✅ Model loaded successfully!")
        
        # 9. MAKE PREDICTIONS
        predictions = model.transform(feature_df)
        
        # 10. EXTRACT FRAUD PREDICTIONS
        # The model outputs 'prediction' column (0 or 1)
        fraud_df = predictions.select(
            "user_id",
            "amount",
            "location",
            "timestamp",
            "time_diff",
            "is_foreign",
            "prediction",  # 0 = legit, 1 = fraud
            "probability"  # Probability vector
        ).withColumn(
            "fraud_score",
            col("probability").getItem(1)  # Probability of fraud
        )
        
        # Filter only fraud transactions (prediction == 1)
        fraud_alerts = fraud_df.filter(col("prediction") == 1.0)
        
    except Exception as e:
        print(f"⚠️ Could not load model: {e}")
        print("Using simple rule-based detection as fallback...")
        # Fallback to simple rule
        fraud_df = standardized_df.withColumn("prediction", (col("amount") > 1000).cast("double"))
        fraud_alerts = fraud_df.filter(col("prediction") == 1.0)

else:
    print("⚠️ No model found in MLflow. Using simple rule-based detection...")
    fraud_df = standardized_df.withColumn("prediction", (col("amount") > 1000).cast("double"))
    fraud_alerts = fraud_df.filter(col("prediction") == 1.0)

# 11. OUTPUT STREAMS

# Stream 1: Print fraud alerts to console
# Stream 1: Print fraud alerts to console
console_query = fraud_alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start()

# Stream 3: Send ALL predictions to Kafka (For Dashboard)
# This includes both fraud AND normal transactions
kafka_all_query = fraud_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "all_predictions") \
    .option("checkpointLocation", "s3a://transactions/checkpoints/all_predictions/") \
    .trigger(processingTime='5 seconds') \
    .start()

# Stream 4: Send fraud alerts separately (For alerts system)
kafka_alert_query = fraud_alerts.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "s3a://transactions/checkpoints/alerts/") \
    .trigger(processingTime='5 seconds') \
    .start()

# Stream 2: Save all processed data to MinIO for retraining
minio_query = standardized_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://transactions/data/") \
    .option("checkpointLocation", "s3a://transactions/checkpoints/") \
    .trigger(processingTime='5 seconds') \
    .start()

print("✅ Real-time fraud detection is running!")
print("📊 Fraud alerts will appear below...")
print(f"💾 All transactions being saved to MinIO for future training")

spark.streams.awaitAnyTermination()
