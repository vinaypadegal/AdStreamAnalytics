from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import csv
import time

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ad_events"
NUM_PARTITIONS = 8
REPLICATION_FACTOR = 1

STREAMING_RATE_PER_SECOND = 1000

# Function to Create Kafka Topic if Not Exists
def create_topic_if_not_exists():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    existing_topics = admin_client.list_topics()
    
    if TOPIC_NAME not in existing_topics:
        topic = NewTopic(name=TOPIC_NAME, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
        admin_client.create_topics([topic])
        print(f"Created topic: {TOPIC_NAME}")
    else:
        print(f"Topic {TOPIC_NAME} already exists.")

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=str.encode,
)

# Read CSV and Send Messages to Kafka
def stream_ad_events(csv_file, sleep_time=1/STREAMING_RATE_PER_SECOND):
    with open(csv_file, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            message = {
                "event_id": row["event_id"],
                "timestamp": time.time(),
                "event_type": row["event_type"],
                "user_id": row["user_id"],
                "ad_id": row["ad_id"],
                "campaign_id": row["campaign_id"],
                "cost_per_click": float(row["cost_per_click"]) if row["cost_per_click"] else None,
            }

            producer.send(TOPIC_NAME, key=row["campaign_id"], value=message)
            print(f"Produced: {message}")

            time.sleep(sleep_time)  # Simulate real-time streaming

    print("Finished streaming events.")

# Run Producer
if __name__ == "__main__":
    create_topic_if_not_exists()
    stream_ad_events("synthetic_ad_events.csv")
    # stream_ad_events("synthetic_ads_100k.csv")



