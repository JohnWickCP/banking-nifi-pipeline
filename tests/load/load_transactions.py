"""
load_transactions.py — Send 10,000 transactions (with fraud patterns) then measure
how long until all records appear in fact_txn. Used for clean benchmark runs.

Fraud mix:
  - 8 velocity clusters (4 txns each = 32 txns)  → Rule 1 alerts
  - 15 off-hours large txns (amount > 50M VND)    → Rule 3 alerts (if hour < 6 or >= 22 UTC)
  - 10 duplicate pairs (2 txns each = 20 txns)    → Rule 4 alerts
  - remainder: normal transactions

Usage:
    python tests/load/load_transactions.py

Requirements:
    pip install kafka-python psycopg2-binary
    Docker stack running + NiFi flow started
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
    import psycopg2
except ImportError:
    print("ERROR: pip install kafka-python psycopg2-binary")
    sys.exit(1)

BROKER = "localhost:9092"
TOPIC  = "txn.raw"
TOTAL  = 10_000
PREFIX = "BENCH"

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "banking_dw"),
    "user":     os.getenv("PG_USER",     "banking"),
    "password": os.getenv("PG_PASSWORD", "banking123"),
}

PROVINCES = [
    ("Ha Noi",     21.0278, 105.8342),
    ("Ho Chi Minh", 10.8231, 106.6297),
    ("Da Nang",    16.0544, 108.2022),
]
CHANNELS  = ["ATM", "POS", "mobile", "internet"]
# Fixed home province per account to prevent false geo_anomaly alerts
ACCOUNT_HOME = {f"****{i:04d}": PROVINCES[i % len(PROVINCES)] for i in range(1000, 9999)}

counter = 0


def make_txn(account=None, amount=None, merchant=None, province=None,
             fraud_label=0, fraud_type=None):
    global counter
    counter += 1
    # Unique account per normal txn prevents false velocity/duplicate alerts.
    # Fraud pattern callers pass an explicit account so clusters stay on same account.
    acct = account or f"****{counter:05d}"
    prov = province or PROVINCES[counter % len(PROVINCES)]
    ts   = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return {
        "transaction_id":    f"{PREFIX}-{counter:09d}",
        "timestamp":         ts,
        "account_masked":    acct,
        "customer_id":       f"CUS{(counter % 5000 + 1):06d}",
        "channel":           CHANNELS[counter % 4],
        "merchant_id":       merchant or f"MER{(counter % 5000 + 1):05d}",
        "merchant_province": prov[0],
        "merchant_lat":      prov[1],
        "merchant_lon":      prov[2],
        "amount":            amount or (counter % 20 + 1) * 1_000_000,
        "currency":          "VND",
        "status":            "SUCCESS",
        "fraud_label":       fraud_label,
        "fraud_type":        fraud_type,
    }


def send_velocity_cluster(producer, acct):
    for _ in range(4):
        producer.send(TOPIC, make_txn(account=acct, fraud_label=1, fraud_type="velocity"))


def send_off_hours_large(producer):
    producer.send(TOPIC, make_txn(amount=75_000_000, fraud_label=1, fraud_type="off_hours_large"))


def send_duplicate_pair(producer, acct, amt, mer):
    for _ in range(2):
        producer.send(TOPIC, make_txn(account=acct, amount=amt, merchant=mer,
                                      fraud_label=1, fraud_type="duplicate"))


def wait_for_postgres(expected, timeout_s=300):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    t0 = time.perf_counter()
    last = 0
    while True:
        cur.execute(f"SELECT COUNT(*) FROM fact_txn WHERE transaction_id LIKE '{PREFIX}%'")
        count = cur.fetchone()[0]
        elapsed = time.perf_counter() - t0
        if count != last:
            rate = count / elapsed if elapsed > 0 else 0
            print(f"  [{elapsed:6.1f}s]  {count:>6,}/{expected:,} rows  ({rate:.0f} rows/s)")
            last = count
        if count >= expected:
            cur.close(); conn.close()
            return count, elapsed
        if elapsed > timeout_s:
            cur.close(); conn.close()
            print(f"  TIMEOUT after {timeout_s}s — {count}/{expected} rows")
            return count, elapsed
        time.sleep(1)


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=65536,
    )

    # Build 10,000 record batch with fraud patterns
    # Fraud positions: every 1000 records, inject a pattern
    VELOCITY_ACCOUNTS  = [f"****VL{i:02d}" for i in range(8)]
    DUPLICATE_SPECS    = [(f"****DP{i:02d}", (i+1)*2_000_000, f"MER{i:05d}") for i in range(10)]

    print(f"[load-transactions] Building {TOTAL:,} records (with fraud patterns)...")
    t_start = time.perf_counter()

    normal_sent  = 0
    fraud_events = {"velocity": 0, "off_hours_large": 0, "duplicate": 0}

    v_idx = 0  # velocity cluster index
    d_idx = 0  # duplicate pair index

    for i in range(1, TOTAL + 1):
        if i % 1000 == 0 and v_idx < len(VELOCITY_ACCOUNTS):
            # Inject velocity cluster
            send_velocity_cluster(producer, VELOCITY_ACCOUNTS[v_idx])
            fraud_events["velocity"] += 1
            v_idx += 1
        elif i % 700 == 0:
            # Inject off-hours large
            send_off_hours_large(producer)
            fraud_events["off_hours_large"] += 1
        elif i % 900 == 0 and d_idx < len(DUPLICATE_SPECS):
            # Inject duplicate pair
            a, amt, mer = DUPLICATE_SPECS[d_idx]
            send_duplicate_pair(producer, a, amt, mer)
            fraud_events["duplicate"] += 1
            d_idx += 1
        else:
            producer.send(TOPIC, make_txn())
            normal_sent += 1

    producer.flush()
    producer.close()

    total_sent = counter
    produce_s  = time.perf_counter() - t_start
    print(f"  Kafka produce: {total_sent:,} records in {produce_s:.2f}s ({total_sent/produce_s:.0f} records/s)")
    print(f"  Fraud patterns: velocity={fraud_events['velocity']} clusters, "
          f"off_hours={fraud_events['off_hours_large']}, "
          f"duplicate={fraud_events['duplicate']} pairs")
    print()

    print(f"[load-transactions] Waiting for {total_sent:,} rows in fact_txn...")
    print("  (NiFi: Kafka -> LookupRecord -> velocity check -> duplicate check -> QueryRecord -> PutDB)")
    print()

    arrived, elapsed = wait_for_postgres(total_sent)
    throughput = arrived / elapsed if elapsed > 0 else 0

    print()
    print("=" * 60)
    print("  LOAD TEST RESULT")
    print(f"  Records sent to Kafka : {total_sent:>8,}")
    print(f"  Records in fact_txn   : {arrived:>8,}")
    print(f"  End-to-end time       : {elapsed:>8.2f}s")
    print(f"  NiFi throughput       : {throughput:>8.0f} rows/s")
    print("=" * 60)
    print()
    print("  Baseline Python ETL : 4.16s | 10,000 rows | 2,405 rows/s")
    print(f"  NiFi Pipeline       : {elapsed:.2f}s | {arrived:,} rows | {throughput:.0f} rows/s")
    print()
    print("  Check fraud alerts:")
    print('    docker exec banking-postgres psql -U banking -d banking_dw -c "SELECT rule_triggered, COUNT(*) FROM fact_alert GROUP BY 1;"')


if __name__ == "__main__":
    main()
