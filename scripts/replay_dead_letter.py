"""
replay_dead_letter.py — Replay tất cả messages từ txn.dead-letter → txn.raw
Usage: python scripts/replay_dead_letter.py
"""
from kafka import KafkaConsumer, KafkaProducer
import sys

BROKER = "localhost:9092"
SOURCE = "txn.dead-letter"
TARGET = "txn.raw"

consumer = KafkaConsumer(
    SOURCE,
    bootstrap_servers=BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
)
producer = KafkaProducer(bootstrap_servers=BROKER)

count = 0
for msg in consumer:
    producer.send(TARGET, value=msg.value, key=msg.key)
    count += 1
    if count % 100 == 0:
        print(f"  Replayed {count} messages...")

producer.flush()
producer.close()
consumer.close()
print(f"Done — replayed {count} messages from {SOURCE} → {TARGET}")
