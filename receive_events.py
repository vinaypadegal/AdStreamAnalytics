from kafka import KafkaConsumer
import json

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ad_events"
GROUP_ID = "ad_analytics_group"  # Consumer Group

# Create Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",  # Start from beginning if no previous offset
    enable_auto_commit=True,  # Automatically commit offsets
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None
)

print(f"Consumer is listening on topic: {TOPIC_NAME}...")

# Consume Messages
try:
    for message in consumer:
        print(f"Received: {message.value}")  # Print message
except KeyboardInterrupt:
    print("\nConsumer interrupted. Exiting gracefully...")
finally:
    consumer.close()
    print("Consumer closed.")
