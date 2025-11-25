# Real-Time Fraud Detection with Schema Mapping

## 📋 Overview

This system handles **multiple data sources** with different formats and uses **ML model** for real-time fraud detection.

---

## 🏗️ Architecture

```
Data Sources (Various Formats)
    ↓
Kafka Topic: "transactions"
    ↓
Spark Streaming (spark_inference.py)
    ├─ Step 1: Schema Mapping
    ├─ Step 2: Feature Engineering
    ├─ Step 3: Load ML Model from MLflow
    ├─ Step 4: Predict Fraud
    └─ Step 5: Alert if Fraud
    ↓
Outputs:
- Console (Fraud Alerts)
- MinIO (All Data for Retraining)
```

---

## 📁 Files Created

### 1. **schema_config.py**
- Defines mappings for different data sources
- **Add new sources here!**

### 2. **spark_inference.py**
- Main real-time processing pipeline
- Loads model from MLflow
- Makes predictions

### 3. **test_formats.py**
- Test script to simulate different data formats
- Use this to verify mapping works

---

## 🚀 How to Use

### Step 1: Choose Your Data Source

Edit `spark_inference.py` line 10:
```python
DATA_SOURCE = "synthetic"  # Change to: 'bank_a', 'bank_b', etc.
```

### Step 2: Run the Inference Pipeline

```bash
# Copy to Spark container
docker cp spark_inference.py spark-master:/opt/spark/

# Run it
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /opt/spark/spark_inference.py
```

### Step 3: Send Test Data

**Option A: Use existing producer**
```bash
python producer.py
```

**Option B: Test different formats**
```bash
python test_formats.py
```

---

## 🔧 Adding a New Data Source

### Example: Adding "Bank C"

**Step 1: Add mapping to schema_config.py**
```python
SCHEMA_MAPPINGS = {
    # ... existing mappings ...
    
    "bank_c": {
        "user_id": "client_id",        # They call it 'client_id'
        "amount": "value",               # They call it 'value'
        "location": "geo_location",      # They call it 'geo_location'
        "timestamp": "created_at",       # They call it 'created_at'
        "home_loc": "origin_country"     # They call it 'origin_country'
    }
}
```

**Step 2: Update DATA_SOURCE in spark_inference.py**
```python
DATA_SOURCE = "bank_c"
```

**Step 3: Restart Spark**

That's it! ✅

---

## 📊 How Schema Mapping Works

### Input (Bank A Format):
```json
{
  "customer_number": "C123",
  "transaction_amount_usd": 5000,
  "merchant_country": "Nigeria",
  "event_time": 1635789012,
  "billing_address_country": "USA"
}
```

### Mapping Configuration:
```python
"bank_a": {
    "user_id": "customer_number",
    "amount": "transaction_amount_usd",
    ...
}
```

### Output (Standard Format):
```json
{
  "user_id": "C123",
  "amount": 5000,
  "location": "Nigeria",
  "timestamp": 1635789012,
  "home_loc": "USA"
}
```

### Model Receives:
```
Features: [amount=5000, time_diff=120, is_foreign=1, ...]
    ↓
Prediction: 0.92 (92% fraud probability)
    ↓
Alert: ⚠️ FRAUD DETECTED!
```

---

## 🧠 Feature Engineering (Automatic)

The system automatically calculates these features:

1. **time_diff**: Time since user's last transaction
2. **hour**: Hour of day (0-23)
3. **amount_ratio**: Current amount / user's average amount
4. **is_foreign**: 1 if location ≠ home_loc, else 0

**These are calculated the SAME way as in training!**

---

## 🤖 Model Loading

The system automatically:
1. Connects to MLflow (`http://mlflow:5000`)
2. Finds the latest trained model
3. Loads it into Spark
4. Uses it for predictions

**Fallback:** If model fails to load, uses simple rule: `amount > 1000`

---

## 📈 Monitoring Output

### Console Output (Fraud Alerts):
```
+-------+------+--------+----------+---------+-----------+----------+
|user_id|amount|location|timestamp |time_diff|prediction |fraud_score|
+-------+------+--------+----------+---------+-----------+----------+
|U001   |5000.0|Nigeria |1635789012|120      |1.0        |0.92       |
+-------+------+--------+----------+---------+-----------+----------+
```

### MLflow UI:
- Open: http://localhost:5000
- View: Experiments → fraud_detection_experiment
- See: All trained models with metrics

### MinIO UI:
- Open: http://localhost:9001
- Login: minioadmin / minioadmin
- View: Bucket "transactions" → All processed data

---

## 🔄 Complete Workflow

### Development (Now):
```
producer.py → Kafka → spark_inference.py → Fraud Alerts
```

### Production (Future):
```
Bank API → Kafka → spark_inference.py → Fraud Alerts
    ↓
Just change DATA_SOURCE to match bank's format!
```

---

## ✅ Advantages of This Approach

1. **Flexible**: Support multiple data sources
2. **Centralized**: All mapping in one config file
3. **Testable**: Easy to test with different formats
4. **Scalable**: Add new sources without changing core logic
5. **Production-Ready**: Same code works in dev and prod

---

## 🚨 Important Notes

### Feature Consistency
**Training** and **Inference** must use SAME features:
- ✅ Both calculate `time_diff` the same way
- ✅ Both calculate `is_foreign` the same way
- ❌ If training used `hour` but inference doesn't → MODEL BREAKS

### Data Types
Ensure fields are correct types:
- `amount`: float/double
- `timestamp`: long (unix timestamp)
- `user_id`: string
- `location`: string

### Missing Fields
If incoming data is missing a required field:
- System will throw an error
- Check mapping configuration
- Verify data source sends all fields

---

## 📞 Troubleshooting

### "Model not found"
→ Run `train_model.py` first to create a model

### "Field X not found"
→ Check mapping in `schema_config.py`
→ Verify data source actually sends that field

### "Predictions all 0 or all 1"
→ Check feature engineering matches training
→ Verify model loaded correctly

---

## 🎯 Next Steps

1. ✅ Test with synthetic data (producer.py)
2. ✅ Test with different formats (test_formats.py)
3. ⬜ Connect to real bank API
4. ⬜ Set up automated retraining (Airflow)
5. ⬜ Create monitoring dashboard (Streamlit)

---

**You now have a production-ready schema mapping system!** 🎉
