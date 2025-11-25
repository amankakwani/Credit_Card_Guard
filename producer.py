import time
import json
import random
from kafka import KafkaProducer

# 1. Connect to Kafka
# We try to connect until it succeeds (in case Kafka is slow to start)
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
    except Exception as e:
        print("Waiting for Kafka...", e)
        time.sleep(5)

# 2. Define User Profiles (The "Blueprint")
# We simulate 1000 users with different spending habits
# LOCATION LOGIC: We assign a "Home" to each user randomly from this list.
locations_list = ["NY", "CA", "TX", "FL", "IL", "Delhi", "London", "Tokyo"]
users = []
for i in range(1000):
    users.append({
        "user_id": f"User_{i}",
        "home_loc": random.choice(locations_list), # Assign Home: e.g. "Delhi"
        "avg_spend": random.uniform(10, 100)
    })

# 3. The Infinite Loop (Data Generation)
print("Starting transaction stream...")
while True:
    # Pick a random user
    user = random.choice(users)
    
    # Decide: Is this Fraud? (1% chance)
    is_fraud = random.random() < 0.01
    
    if is_fraud:
        # FRAUD PATTERN:
        # 1. Amount is huge (10x normal)
        # 2. Location is NOT their home (e.g. User from Delhi is suddenly in Russia)
        amount = random.uniform(1000, 5000)
        location = random.choice(["China", "Russia", "Brazil", "Unknown"])
    else:
        # NORMAL PATTERN:
        # 1. Amount is near their average
        # 2. Location is their HOME
        amount = random.gauss(user["avg_spend"], 10) 
        location = user["home_loc"]
    
    # Create the transaction object
    transaction = {
        "user_id": user["user_id"],
        "amount": round(abs(amount), 2),
        "location": location,
        "home_loc": user["home_loc"],  # User's permanent home location
        "timestamp": time.time(),
        "is_fraud": int(is_fraud)
    }
    
    # Send to Kafka Topic 'transactions'
    producer.send('transactions', transaction)
    
    # Print occasionally so we see it working
    if is_fraud:
        print(f"⚠️  FRAUD SENT: {transaction}")
    
    # Sleep to control speed (100 transactions per second)
    time.sleep(0.01)
