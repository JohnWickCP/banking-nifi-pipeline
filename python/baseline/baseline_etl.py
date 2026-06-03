"""
baseline_etl.py — Manual Python ETL baseline for benchmark comparison
Intentionally row-by-row psycopg2 (no COPY, no executemany) to establish
a "before NiFi" throughput number for docs/benchmark.md.

Run from project root:
    python python/baseline/baseline_etl.py

Requires PostgreSQL (start with: docker compose up -d postgres)
Override connection via env vars: PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
"""

import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# ── Connection config (matches docker-compose defaults) ───────────────────────

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "banking_dw"),
    "user":     os.getenv("PG_USER",     "banking"),
    "password": os.getenv("PG_PASSWORD", "banking123"),
}

# ── File config ───────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_FILE   = PROJECT_ROOT / "output" / "txn_all.csv"
SAMPLE_ROWS  = 10_000

# ── SQL ───────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS staging_raw (
    id                SERIAL PRIMARY KEY,
    transaction_id    VARCHAR(20),
    ts                TIMESTAMP,
    account_masked    VARCHAR(20),
    customer_id       VARCHAR(10),
    channel           VARCHAR(20),
    merchant_id       VARCHAR(10),
    merchant_province VARCHAR(50),
    merchant_lat      DOUBLE PRECISION,
    merchant_lon      DOUBLE PRECISION,
    amount            BIGINT,
    currency          CHAR(3),
    status            VARCHAR(20),
    fraud_label       SMALLINT,
    fraud_type        VARCHAR(50),
    loaded_at         TIMESTAMP DEFAULT NOW()
);
"""

TRUNCATE = "TRUNCATE TABLE staging_raw;"

INSERT = """
INSERT INTO staging_raw (
    transaction_id, ts, account_masked, customer_id, channel,
    merchant_id, merchant_province, merchant_lat, merchant_lon,
    amount, currency, status, fraud_label, fraud_type
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_csv(filepath: Path, n: int) -> list[dict]:
    rows = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= n:
                break
            rows.append(row)
    return rows


def parse_row(row: dict) -> tuple:
    fraud_type = row.get("fraud_type", "")
    return (
        row["transaction_id"],
        datetime.fromisoformat(row["timestamp"]),
        row["account_masked"],
        row["customer_id"],
        row["channel"],
        row["merchant_id"],
        row["merchant_province"],
        float(row["merchant_lat"]) if row["merchant_lat"] else None,
        float(row["merchant_lon"]) if row["merchant_lon"] else None,
        int(row["amount"]),
        row["currency"],
        row["status"],
        int(row["fraud_label"]),
        fraud_type if fraud_type not in ("", "nan", "None") else None,
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run_baseline() -> None:
    print(f"[baseline_etl] input  : {INPUT_FILE}")
    print(f"[baseline_etl] sample : {SAMPLE_ROWS:,} rows")
    print(f"[baseline_etl] target : {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print()

    # 1. Load CSV
    t0 = time.perf_counter()
    rows = load_csv(INPUT_FILE, SAMPLE_ROWS)
    t_load = time.perf_counter() - t0
    print(f"  CSV loaded    : {len(rows):,} rows in {t_load:.2f}s")

    # 2. Connect
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        print(f"\nERROR: cannot connect to PostgreSQL — {e}")
        print("Start with: docker compose up -d postgres")
        sys.exit(1)

    conn.autocommit = False
    cur = conn.cursor()

    # 3. Prepare table
    cur.execute(DDL)
    cur.execute(TRUNCATE)
    conn.commit()
    print(f"  Table ready   : staging_raw (truncated)")
    print()
    print(f"  Inserting {len(rows):,} rows one-by-one (no COPY, no executemany)...")
    print(f"  {'Progress':>8}  {'Elapsed':>8}  {'Rate':>10}")
    print(f"  {'-'*8}  {'-'*8}  {'-'*10}")

    # 4. Row-by-row insert — intentionally unoptimized
    errors = 0
    t_start = time.perf_counter()

    for i, row in enumerate(rows):
        try:
            cur.execute(INSERT, parse_row(row))
        except Exception as e:
            errors += 1
            conn.rollback()
            if errors <= 5:
                print(f"  [WARN] row {i}: {e}")
            continue

        # commit every 500 rows
        if (i + 1) % 500 == 0:
            conn.commit()
            elapsed = time.perf_counter() - t_start
            rate    = (i + 1) / elapsed
            print(f"  {i+1:>7,}   {elapsed:>7.1f}s  {rate:>9.0f}/s")

    conn.commit()
    t_end = time.perf_counter()
    cur.close()
    conn.close()

    # 5. Results
    total_s  = t_end - t_start
    inserted = len(rows) - errors
    rate     = inserted / total_s

    print()
    print("=" * 55)
    print("  BASELINE RESULT")
    print(f"  Rows attempted : {len(rows):>8,}")
    print(f"  Rows inserted  : {inserted:>8,}")
    print(f"  Errors         : {errors:>8,}")
    print(f"  Total time     : {total_s:>8.2f}s")
    print(f"  Throughput     : {rate:>8.0f} rows/s")
    print("=" * 55)
    print()
    print("  → Copy this line into docs/benchmark.md:")
    print(f"    Baseline Python ETL : {total_s:.2f}s | {inserted:,} rows | {rate:.0f} rows/s")


if __name__ == "__main__":
    run_baseline()
