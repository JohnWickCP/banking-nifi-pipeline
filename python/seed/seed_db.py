"""
seed_db.py — Seed PostgreSQL with dimension tables
Reads output/dim_customer.csv → upserts dim_customer (5,000 rows)
Generates dim_calendar 2024-2026 with VN holidays

Run after: docker compose up -d postgres
    python python/seed/seed_db.py

Screenshot target: SELECT COUNT(*) FROM dim_customer  → 5000
"""

import csv
import os
import sys
from datetime import date, timedelta
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "banking_dw"),
    "user":     os.getenv("PG_USER",     "banking"),
    "password": os.getenv("PG_PASSWORD", "banking123"),
}

PROJECT_ROOT  = Path(__file__).resolve().parents[2]
DIM_CUSTOMER  = PROJECT_ROOT / "output" / "dim_customer.csv"

# VN public holidays 2025 from MASTER_SPEC
VN_HOLIDAYS_2025 = {
    date(2025, 1, 1):  "Tết Dương lịch",
    date(2025, 1, 28): "Tết Nguyên Đán (nghỉ bù)",
    date(2025, 1, 29): "Tết Nguyên Đán",
    date(2025, 1, 30): "Tết Nguyên Đán",
    date(2025, 1, 31): "Tết Nguyên Đán",
    date(2025, 2, 1):  "Tết Nguyên Đán",
    date(2025, 2, 2):  "Tết Nguyên Đán",
    date(2025, 2, 3):  "Tết Nguyên Đán",
    date(2025, 4, 7):  "Giỗ Tổ Hùng Vương",
    date(2025, 4, 30): "Ngày Giải phóng miền Nam",
    date(2025, 5, 1):  "Ngày Quốc tế Lao động",
    date(2025, 9, 2):  "Ngày Quốc khánh",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def connect() -> psycopg2.extensions.connection:
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        print(f"\nERROR: cannot connect — {e}")
        print("Start with: docker compose up -d postgres")
        sys.exit(1)


def seed_dim_customer(conn) -> int:
    if not DIM_CUSTOMER.exists():
        print(f"  [SKIP] {DIM_CUSTOMER} not found — run data_generator.py first")
        return 0

    rows = []
    with open(DIM_CUSTOMER, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append((
                row["customer_id"],
                row["account_masked"],
                row["segment"],
                int(row["risk_score"]),
                row["home_province"],
                float(row["home_lat"]),
                float(row["home_lon"]),
            ))

    sql = """
        INSERT INTO dim_customer
            (customer_id, account_masked, segment, risk_score,
             home_province, home_lat, home_lon)
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            account_masked = EXCLUDED.account_masked,
            segment        = EXCLUDED.segment,
            risk_score     = EXCLUDED.risk_score,
            home_province  = EXCLUDED.home_province,
            home_lat       = EXCLUDED.home_lat,
            home_lon       = EXCLUDED.home_lon
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)


def seed_dim_calendar(conn, start_year: int = 2024, end_year: int = 2026) -> int:
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)

    rows = []
    d = start
    while d <= end:
        dow         = d.weekday()           # 0=Mon, 6=Sun
        is_weekend  = dow >= 5
        is_holiday  = d in VN_HOLIDAYS_2025
        holiday_name = VN_HOLIDAYS_2025.get(d)
        is_maint    = (dow == 6)            # Sunday = maintenance window (1-3 AM)

        rows.append((
            int(d.strftime("%Y%m%d")),
            d,
            dow,
            is_weekend,
            is_holiday,
            holiday_name,
            is_maint,
        ))
        d += timedelta(days=1)

    sql = """
        INSERT INTO dim_calendar
            (date_id, full_date, day_of_week, is_weekend,
             is_holiday, holiday_name, is_maintenance)
        VALUES %s
        ON CONFLICT (date_id) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)


def seed_dim_time(conn) -> int:
    """Run 03_seed_dim_time.sql if dim_time is empty."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dim_time")
        count = cur.fetchone()[0]
        if count > 0:
            return 0  # already seeded

    sql_file = PROJECT_ROOT / "sql" / "03_seed_dim_time.sql"
    with open(sql_file, encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    return 1440


def verify(conn) -> None:
    queries = [
        ("dim_customer",  "SELECT COUNT(*) FROM dim_customer"),
        ("dim_calendar",  "SELECT COUNT(*) FROM dim_calendar"),
        ("dim_time",      "SELECT COUNT(*) FROM dim_time"),
        ("fact_txn",      "SELECT COUNT(*) FROM fact_txn"),
        ("staging_raw",   "SELECT COUNT(*) FROM staging_raw"),
    ]
    print()
    print("=" * 45)
    print("  VERIFICATION — copy to screenshot 02")
    print("=" * 45)
    with conn.cursor() as cur:
        for label, q in queries:
            cur.execute(q)
            n = cur.fetchone()[0]
            print(f"  {label:<18} {n:>8,} rows")
    print("=" * 45)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("[seed_db] Connecting to PostgreSQL...")
    conn = connect()
    print(f"  Connected: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print()

    print("[1/3] Seeding dim_customer...")
    n = seed_dim_customer(conn)
    print(f"  Upserted {n:,} customers")

    print("[2/3] Seeding dim_calendar (2024–2026)...")
    n = seed_dim_calendar(conn)
    print(f"  Inserted {n:,} calendar days ({n // 365} years)")

    print("[3/3] Seeding dim_time (1440 minutes)...")
    n = seed_dim_time(conn)
    if n:
        print(f"  Inserted {n:,} time rows")
    else:
        print("  Already seeded — skipped")

    verify(conn)
    conn.close()


if __name__ == "__main__":
    main()
