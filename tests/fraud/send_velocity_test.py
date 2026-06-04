"""
Test script for Fraud Rule 1 — Velocity Check
Sends 3 transactions from the same account within 60 seconds.
Expected: tx1 and tx2 pass, tx3 triggers velocity alert.

Usage:
    python tests/fraud/send_velocity_test.py

Requirements:
    pip install kafka-python
    Docker stack running (docker compose up -d)
"""

import json
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC  = "txn.raw"


def make_txn(n: int, account: str) -> dict:
    return {
        "transaction_id":   f"VELTEST-{account[-4:]}-{n:03d}",
        "timestamp":        datetime.utcnow().isoformat(timespec="seconds"),
        "account_masked":   account,
        "customer_id":      "CUS000001",
        "channel":          "ATM",
        "merchant_id":      "MER00001",
        "merchant_province": "Ha Noi",
        "merchant_lat":     21.0278,
        "merchant_lon":     105.8342,
        "amount":           5_000_000,
        "currency":         "VND",
        "status":           "SUCCESS",
        "fraud_label":      0,
        "fraud_type":       None,
    }


def run_test():
    test_account = "****VT" + str(uuid.uuid4())[:2].upper()
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Sending 3 transactions for account {test_account} within 60-second window...")
    for i in range(1, 4):
        txn = make_txn(i, test_account)
        producer.send(TOPIC, txn)
        producer.flush()
        print(f"  [{i}/3] Sent {txn['transaction_id']} — amount {txn['amount']:,} VND")
        if i < 3:
            time.sleep(1)   # stay well inside the 60s window

    producer.close()
    print("\nDone. Check results:")
    print("  psql -U banking -d banking_dw -c \"SELECT transaction_id, fraud_flag, alert_id FROM fact_txn WHERE transaction_id LIKE 'VELTEST%' ORDER BY loaded_at;\"")
    print("  psql -U banking -d banking_dw -c \"SELECT * FROM fact_alert WHERE rule_triggered = 'velocity' ORDER BY detected_at DESC LIMIT 3;\"")


if __name__ == "__main__":
    run_test()
