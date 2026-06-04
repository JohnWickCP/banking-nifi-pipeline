"""
send_bad_record.py — Gửi record thiếu field để test dead-letter routing
"""
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Record thiếu field bắt buộc (không có amount, currency, status)
bad_record = {
    "transaction_id": "BAD-TEST-001",
    "timestamp": "2025-06-04T09:00:00",
    "account_masked": "****9999",
    "channel": "ATM",
}

producer.send("txn.raw", bad_record)
producer.flush()
producer.close()
print("Sent 1 bad record → check txn.dead-letter topic and NiFi dead-letter processor")
