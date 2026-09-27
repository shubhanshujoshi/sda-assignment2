import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC = "ecommerce_orders"

# MongoDB configuration
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ecommerce_streaming")

if not MONGODB_URI:
    raise ValueError("MONGODB_URI is not configured in .env")

# Connect to MongoDB Atlas
mongo_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=10000
)

# Verify MongoDB connection
mongo_client.admin.command("ping")

db = mongo_client[MONGODB_DB]
orders_collection = db["orders"]

# Create a unique index on Order_ID
orders_collection.create_index(
    [("Order_ID", ASCENDING)],
    unique=True
)

print("Connected to MongoDB Atlas.")
print(f"Database: {MONGODB_DB}")
print("Collection: orders")

# Create Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="assignment3-history-v1"
)

print("Connected to Kafka.")
print(f"Listening to topic: {TOPIC}")
print("-" * 70)

try:
    for message in consumer:

        # Decode Kafka message
        order = json.loads(message.value.decode("utf-8"))

        # Display the consumed order
        print(
            f"Received: {order['Order_ID']} | "
            f"{order['Product_Name']} | "
            f"₹{order['Total_Amount']} | "
            f"{order['City']} | "
            f"Status: {order['Payment_Status']}"
        )

        # Store/update the order in MongoDB
        result = orders_collection.update_one(
            {"Order_ID": order["Order_ID"]},
            {"$set": order},
            upsert=True
        )

        if result.upserted_id:
            print(f"Stored in MongoDB: {order['Order_ID']} (new)")
        else:
            print(f"Stored in MongoDB: {order['Order_ID']} (updated)")

        print("-" * 70)

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
    mongo_client.close()
    print("Connections closed.")
