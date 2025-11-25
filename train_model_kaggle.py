from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import mlflow
import mlflow.spark
import os

# Set AWS credentials for MLflow artifact storage
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://minio:9000'

print("=" * 60)
print("🎯 Kaggle Credit Card Fraud Detection - Model Training")
print("=" * 60)

# 1. Initialize Spark
print("\n📦 Initializing Spark...")
spark = SparkSession.builder \
    .appName("FraudDetectorKaggle") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("✅ Spark initialized!")

# 2. Load Kaggle dataset from MinIO
print("\n📊 Loading Kaggle credit card fraud dataset from MinIO...")
df = spark.read.csv(
    "s3a://transactions/creditcard.csv",
    header=True,
    inferSchema=True
)

total_count = df.count()
fraud_count = df.filter("Class = 1").count()
normal_count = df.filter("Class = 0").count()

print(f"✅ Loaded {total_count:,} transactions")
print(f"   ├─ Fraud: {fraud_count:,} ({fraud_count/total_count*100:.2f}%)")
print(f"   └─ Normal: {normal_count:,} ({normal_count/total_count*100:.2f}%)")

# 3. Prepare features
print("\n🧠 Preparing features...")
# Kaggle dataset has V1-V28 (PCA-transformed features) + Amount + Time
feature_cols = [f"V{i}" for i in range(1, 29)] + ["Amount"]
print(f"   Using {len(feature_cols)} features: V1-V28 + Amount")

# Assemble features into a single vector column
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = assembler.transform(df)

# 4. Split data (80% train, 20% test)
print("\n📚 Splitting data...")
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

train_count = train_data.count()
test_count = test_data.count()
print(f"   ├─ Training set: {train_count:,} transactions ({train_count/total_count*100:.1f}%)")
print(f"   └─ Test set: {test_count:,} transactions ({test_count/total_count*100:.1f}%)")

# 5. Train Random Forest
print("\n🌳 Training Random Forest Classifier...")
print("   (This may take 2-5 minutes...)")

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="Class",
    numTrees=20,
    maxDepth=10,
    seed=42
)

model = rf.fit(train_data)
print("✅ Training complete!")

# 6. Evaluate on test set
print("\n📊 Evaluating model on test set...")
predictions = model.transform(test_data)

# Calculate AUC (Area Under ROC Curve)
evaluator = BinaryClassificationEvaluator(labelCol="Class", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)

print(f"🎯 Model Performance:")
print(f"   └─ AUC (Area Under ROC): {auc:.4f}")

# Show fraud detection statistics
test_fraud = test_data.filter("Class = 1").count()
detected_fraud = predictions.filter("Class = 1 AND prediction = 1.0").count()
detection_rate = (detected_fraud / test_fraud * 100) if test_fraud > 0 else 0

print(f"\n🚨 Fraud Detection:")
print(f"   ├─ Total frauds in test set: {test_fraud}")
print(f"   ├─ Frauds detected: {detected_fraud}")
print(f"   └─ Detection rate: {detection_rate:.1f}%")

# Show sample predictions
print("\n📋 Sample Predictions (First 10):")
predictions.select("Class", "prediction", "probability").show(10, truncate=False)

# 7. Save to MLflow
print("\n💾 Saving model to MLflow...")
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("kaggle_fraud_detection")

with mlflow.start_run():
    # Log parameters
    mlflow.log_param("num_trees", 20)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("dataset", "kaggle_creditcard")
    mlflow.log_param("total_transactions", total_count)
    mlflow.log_param("train_size", train_count)
    mlflow.log_param("test_size", test_count)
    mlflow.log_param("fraud_percentage", fraud_count/total_count*100)
    
    # Log metrics
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("fraud_detection_rate", detection_rate)
    mlflow.log_metric("total_frauds", fraud_count)
    
    # Log model
    mlflow.spark.log_model(model, "kaggle_fraud_model")
    
    print("✅ Model saved to MLflow!")
    print(f"   └─ Experiment: kaggle_fraud_detection")
    print(f"   └─ Model: kaggle_fraud_model")

print("\n" + "=" * 60)
print("🎉 Kaggle Model Training Complete!")
print("=" * 60)
print(f"📊 Summary:")
print(f"   ├─ Dataset: 284,807 real fraud transactions")
print(f"   ├─ AUC Score: {auc:.4f}")
print(f"   ├─ Detection Rate: {detection_rate:.1f}%")
print(f"   └─ Model saved to MLflow ✅")
print("\n💡 View results at: http://localhost:5000")
print("=" * 60)

spark.stop()
