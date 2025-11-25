import mlflow
import os

# Set MLflow tracking URI
mlflow.set_tracking_uri("http://mlflow:5000")

# Test creating an experiment and logging a simple metric
mlflow.set_experiment("test_experiment")

print("Testing MLflow connection...")
with mlflow.start_run():
    mlflow.log_param("test_param", "hello")
    mlflow.log_metric("test_metric", 42)
    print("✅ Successfully logged to MLflow!")

print("✅ MLflow is working correctly!")
