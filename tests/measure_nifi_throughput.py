"""
measure_nifi_throughput.py — Đo NiFi pipeline throughput để điền vào benchmark.md
Gửi 10,000 transactions vào Kafka, đợi tất cả xuất hiện trong PostgreSQL, tính thời gian.

Usage:
    python tests/measure_nifi_throughput.py

Requirements:
    pip install kafka-python psycopg2-binary
    Docker stack running + NiFi flow started
"""

import json
import os
import sys
import time
from datetime import datetime

try:
    from kafka import KafkaProducer
    import psycopg2
except ImportError:
    print("ERROR: pip install kafka-python psycopg2-binary")
    sys.exit(1)

BROKER      = "localhost:9092"
TOPIC       = "txn.raw"
BATCH_SIZE  = 10_000
BATCH_PREFIX = "NIFI-BENCH"

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "banking_dw"),
    "user":     os.getenv("PG_USER",     "banking"),
    "password": os.getenv("PG_PASSWORD", "banking123"),
}


def make_txn(i: int) -> dict:
    # Vary account to avoid velocity false-positives hitting Rule 1
    account = f"****{(i % 1000):04d}"
    return {
        "transaction_id":    f"{BATCH_PREFIX}-{i:06d}",
        "timestamp":         datetime.utcnow().isoformat(timespec="seconds"),
        "account_masked":    account,
        "customer_id":       f"CUS{(i % 5000 + 1):06d}",
        "channel":           ["ATM", "POS", "mobile", "internet"][i % 4],
        "merchant_id":       f"MER{(i % 1000 + 1):05d}",
        "merchant_province": "Ha Noi",
        "merchant_lat":      21.0278,
        "merchant_lon":      105.8342,
        "amount":            (i % 10 + 1) * 1_000_000,   # 1M–10M VND
        "currency":          "VND",
        "status":            "SUCCESS",
        "fraud_label":       0,
        "fraud_type":        None,
    }


def send_batch(n: int) -> float:
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=65536,
    )
    t0 = time.perf_counter()
    for i in range(n):
        producer.send(TOPIC, make_txn(i))
    producer.flush()
    producer.close()
    return time.perf_counter() - t0


def wait_for_postgres(expected: int, timeout_s: int = 300) -> tuple[int, float]:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    t0 = time.perf_counter()
    last_count = 0
    while True:
        cur.execute(
            f"SELECT COUNT(*) FROM fact_txn WHERE transaction_id LIKE '{BATCH_PREFIX}%'"
        )
        count = cur.fetchone()[0]
        elapsed = time.perf_counter() - t0
        if count != last_count:
            rate = count / elapsed if elapsed > 0 else 0
            print(f"  [{elapsed:6.1f}s]  {count:>6,}/{expected:,} rows in PostgreSQL  ({rate:.0f} rows/s)")
            last_count = count
        if count >= expected:
            cur.close()
            conn.close()
            return count, elapsed
        if elapsed > timeout_s:
            cur.close()
            conn.close()
            print(f"  TIMEOUT after {timeout_s}s — only {count}/{expected} rows arrived")
            return count, elapsed
        time.sleep(1)


def main():
    print(f"[nifi-throughput] Sending {BATCH_SIZE:,} transactions to Kafka topic '{TOPIC}'")
    send_elapsed = send_batch(BATCH_SIZE)
    print(f"  Kafka produce done: {send_elapsed:.2f}s ({BATCH_SIZE/send_elapsed:.0f} records/s)")
    print()
    print(f"[nifi-throughput] Waiting for {BATCH_SIZE:,} rows to appear in fact_txn...")
    print(f"  (NiFi processes: validate -> Jolt -> LookupRecord -> fraud checks -> PutDatabaseRecord)")
    print()

    arrived, elapsed = wait_for_postgres(BATCH_SIZE)
    throughput = arrived / elapsed if elapsed > 0 else 0

    print()
    print("=" * 60)
    print("  NIFI THROUGHPUT RESULT")
    print(f"  Rows sent to Kafka : {BATCH_SIZE:>8,}")
    print(f"  Rows in fact_txn   : {arrived:>8,}")
    print(f"  Total elapsed      : {elapsed:>8.2f}s")
    print(f"  Throughput         : {throughput:>8.0f} rows/s")
    print("=" * 60)
    print()
    print("  Baseline Python ETL : 4.16s | 10,000 rows | 2,405 rows/s")
    print(f"  NiFi Pipeline      : {elapsed:.2f}s | {arrived:,} rows | {throughput:.0f} rows/s")
    if throughput > 0:
        # NiFi trades raw speed for enrichment + fraud detection
        ratio = 2405 / throughput
        print(f"  Note: NiFi does enrichment + 2 fraud rules + MinIO write")
        print(f"  Throughput ratio (baseline/NiFi): {ratio:.1f}x")
    print()
    print("  -> Copy into docs/benchmark.md:")
    print(f"    NiFi Pipeline : {elapsed:.2f}s | {arrived:,} rows | {throughput:.0f} rows/s")


if __name__ == "__main__":
    main()
