import csv
import json
import time
from kafka import KafkaProducer

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC = "ecommerce_orders"
CSV_FILE = "ecommerce_orders_sample.csv"

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Connected to Kafka.")
print(f"Streaming data to topic: {TOPIC}")
print("-" * 60)

# Read CSV and send each order to Kafka
with open(CSV_FILE, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for order in reader:

        # Convert numeric fields from strings to numbers
        order["Quantity"] = int(order["Quantity"])
        order["Unit_Price"] = float(order["Unit_Price"])
        order["Total_Amount"] = float(order["Total_Amount"])

        # Send order event to Kafka
        future = producer.send(TOPIC, value=order)

        # Wait for Kafka acknowledgement
        metadata = future.get(timeout=10)

        print(
            f"Sent: {order['Order_ID']} | "
            f"{order['Product_Name']} | "
            f"₹{order['Total_Amount']} | "
            f"{order['City']} | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset}"
        )

        # Simulate continuous streaming
        time.sleep(1)

producer.flush()
producer.close()

print("-" * 60)
print("All order events have been sent successfully.")
