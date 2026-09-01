import json
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC = "ecommerce_orders"

# Create Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="assignment2-consumer-v2"
)

print("Connected to Kafka.")
print(f"Listening to topic: {TOPIC}")
print("-" * 70)

try:
    for message in consumer:

        # Decode Kafka message
        order = json.loads(message.value.decode("utf-8"))

        print(
            f"Received: {order['Order_ID']} | "
            f"{order['Product_Name']} | "
            f"₹{order['Total_Amount']} | "
            f"{order['City']} | "
            f"Status: {order['Payment_Status']}"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
