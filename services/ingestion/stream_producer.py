"""
Stream Event Producer (Speed Layer Ingestion)
Simulates real-time NYC Taxi trip events and publishes them to an Apache Kafka broker.
Includes exponential backoff connection retries and rate-controlled event streaming.
"""
import os
import time
import json
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KafkaStreamProducer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips-stream")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "15"))


def connect_kafka_producer_with_retry() -> KafkaProducer:
    """Connects to Kafka broker with exponential backoff retry loop."""
    backoff = 2
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Connecting to Kafka broker at {KAFKA_BOOTSTRAP} (Attempt {attempt}/{MAX_RETRIES})...")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Successfully established connection to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not ready yet. Retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    logger.error("Failed to connect to Kafka broker after maximum retries.")
    raise RuntimeError("Kafka broker unavailable.")


def generate_taxi_trip_event() -> dict:
    """Generates a synthetic, schema-valid timestamped trip event."""
    now = datetime.now(timezone.utc)
    vendor_id = random.choice([1, 2])
    passenger_count = random.randint(1, 4)
    trip_distance = round(random.uniform(0.5, 18.0), 2)
    fare_amount = round(3.50 + (trip_distance * 2.80) + random.uniform(0.0, 5.0), 2)
    tip_amount = round(fare_amount * random.choice([0.0, 0.10, 0.15, 0.20]), 2)
    payment_type = random.choice([1, 2])

    return {
        "trip_id": f"stream_{int(now.timestamp() * 1000)}_{random.randint(100, 999)}",
        "vendor_id": vendor_id,
        "pickup_datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count": passenger_count,
        "trip_distance_miles": trip_distance,
        "fare_amount": fare_amount,
        "tip_amount": tip_amount,
        "payment_type": payment_type,
    }


def stream_events():
    producer = connect_kafka_producer_with_retry()
    sleep_interval = 1.0 / max(EVENTS_PER_SECOND, 0.1)
    logger.info(f"Streaming real-time events to topic '{KAFKA_TOPIC}' at ~{EVENTS_PER_SECOND} events/sec...")

    event_count = 0
    try:
        while True:
            event = generate_taxi_trip_event()
            producer.send(KAFKA_TOPIC, value=event)
            event_count += 1
            if event_count % 50 == 0:
                producer.flush()
                logger.info(f"Published {event_count} events to topic '{KAFKA_TOPIC}'. Latest: Vendor={event['vendor_id']}, Fare=${event['fare_amount']}")
            time.sleep(sleep_interval)
    except KeyboardInterrupt:
        logger.info("Streaming producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer closed. Total events sent: {event_count}")


if __name__ == "__main__":
    stream_events()
