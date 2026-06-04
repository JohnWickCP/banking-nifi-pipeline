"""
Test script for Fraud Rule 4 — Duplicate Detection
Sends 2 transactions with identical account + amount + merchant_id within 30 seconds.
Expected: tx1 passes, tx2 triggers duplicate alert.

Usage:
    python tests/fraud/send_duplicate_test.py

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

# Fixed amount + merchant so the duplicate key matches exactly
DUP_AMOUNT      = 3_500_000
DUP_MERCHANT_ID = "MER00042"


def make_txn(n: int, account: str) -> dict:
    return {
        "transaction_id":    f"DUPTEST-{account[-4:]}-{n:03d}",
        "timestamp":         datetime.utcnow().isoformat(timespec="seconds"),
        "account_masked":    account,
        "customer_id":       "CUS000001",
        "channel":           "ATM",
        "merchant_id":       DUP_MERCHANT_ID,
        "merchant_province": "Ho Chi Minh",
        "merchant_lat":      10.8231,
        "merchant_lon":      106.6297,
        "amount":            DUP_AMOUNT,
        "currency":          "VND",
        "status":            "SUCCESS",
        "fraud_label":       0,
        "fraud_type":        None,
    }


def run_test():
    test_account = "****DT" + str(uuid.uuid4())[:2].upper()
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Sending 2 identical transactions for account {test_account}")
    print(f"  amount={DUP_AMOUNT:,} VND, merchant={DUP_MERCHANT_ID}, window=30s")
    print()
    for i in range(1, 3):
        txn = make_txn(i, test_account)
        producer.send(TOPIC, txn)
        producer.flush()
        print(f"  [{i}/2] Sent {txn['transaction_id']}")
        if i < 2:
            time.sleep(2)   # well inside the 30s duplicate window

    producer.close()
    print("\nDone. Verify results:")
    print("  -- fact_txn: tx1 fraud_flag=false, tx2 fraud_flag=true")
    print("  psql -U banking -d banking_dw -c \"SELECT transaction_id, fraud_flag, alert_id FROM fact_txn WHERE transaction_id LIKE 'DUPTEST%' ORDER BY loaded_at;\"")
    print("  -- fact_alert: rule_triggered=duplicate, severity=LOW")
    print("  psql -U banking -d banking_dw -c \"SELECT * FROM fact_alert WHERE rule_triggered = 'duplicate' ORDER BY detected_at DESC LIMIT 3;\"")


if __name__ == "__main__":
    run_test()
