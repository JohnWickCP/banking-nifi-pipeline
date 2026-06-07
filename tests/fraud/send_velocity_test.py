"""
Test script for Fraud Rule 1 — Velocity Check (sliding window)

Modes:
  fast (default) — 3 txns within 2s: tx3 must trigger alert
  boundary       — t=0, t=30s, t=59s: tx3 must trigger (crosses window boundary)
  expired        — t=0, t=61s: tx2 must NOT trigger (txn1 expired from window)

Usage:
    python tests/fraud/send_velocity_test.py
    python tests/fraud/send_velocity_test.py --boundary
    python tests/fraud/send_velocity_test.py --expired

Requirements:
    pip install kafka-python
    Docker stack running (docker compose up -d)
"""

import json
import sys
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC  = "txn.raw"


def make_txn(n: int, account: str) -> dict:
    return {
        "transaction_id":    f"VELTEST-{account[-4:]}-{n:03d}",
        "timestamp":         datetime.utcnow().isoformat(timespec="seconds"),
        "account_masked":    account,
        "customer_id":       "CUS000001",
        "channel":           "ATM",
        "merchant_id":       "MER00001",
        "merchant_province": "Ha Noi",
        "merchant_lat":      21.0278,
        "merchant_lon":      105.8342,
        "amount":            5_000_000,
        "currency":          "VND",
        "status":            "SUCCESS",
        "fraud_label":       0,
        "fraud_type":        None,
    }


def send(producer, account, n, label=""):
    txn = make_txn(n, account)
    producer.send(TOPIC, txn)
    producer.flush()
    print(f"  [{n}] {txn['transaction_id']}{label}")
    return txn


def run_fast(producer, account):
    print(f"[fast] Account {account} — 3 txns within 2s, tx3 must trigger velocity alert")
    for i in range(1, 4):
        send(producer, account, i)
        if i < 3:
            time.sleep(1)
    print("  Expected: tx3 triggers alert (count=3 within 60s sliding window)")


def run_boundary(producer, account):
    print(f"[boundary] Account {account} — t=0s, t=30s, t=59s")
    print("  Sliding window must detect all 3 even though they straddle window boundaries.")
    send(producer, account, 1, " @ t=0s")
    print("  Sleeping 30s...")
    time.sleep(30)
    send(producer, account, 2, " @ t=30s")
    print("  Sleeping 29s...")
    time.sleep(29)
    send(producer, account, 3, " @ t=59s")
    print("  Expected: tx3 triggers alert (sliding window sees all 3 within last 60s)")


def run_expired(producer, account):
    print(f"[expired] Account {account} — t=0s, t=61s")
    print("  Txn 1 will have expired from the 60s window by the time txn 2 arrives.")
    send(producer, account, 1, " @ t=0s")
    print("  Sleeping 61s...")
    time.sleep(61)
    send(producer, account, 2, " @ t=61s")
    print("  Expected: tx2 does NOT trigger alert (only 1 txn in current 60s window)")


def print_verify(account):
    suffix = account[-4:]
    print()
    print("  Verify results:")
    print(f'    docker exec banking-postgres psql -U banking -d banking_dw -c "SELECT transaction_id, fraud_flag, alert_id FROM fact_txn WHERE transaction_id LIKE \'VELTEST-{suffix}%\' ORDER BY loaded_at;"')
    print(f'    docker exec banking-postgres psql -U banking -d banking_dw -c "SELECT alert_id, transaction_id, rule_triggered, severity, detected_at FROM fact_alert WHERE rule_triggered=\'velocity\' ORDER BY detected_at DESC LIMIT 5;"')


def main():
    mode = "fast"
    if "--boundary" in sys.argv:
        mode = "boundary"
    elif "--expired" in sys.argv:
        mode = "expired"

    account = "****VT" + str(uuid.uuid4())[:2].upper()
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    if mode == "fast":
        run_fast(producer, account)
    elif mode == "boundary":
        run_boundary(producer, account)
    elif mode == "expired":
        run_expired(producer, account)

    producer.close()
    print_verify(account)


if __name__ == "__main__":
    main()
