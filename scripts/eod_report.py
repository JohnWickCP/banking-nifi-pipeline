"""
eod_report.py — End-of-day summary report (chạy lúc 23h55 hoặc thủ công)
Usage: python scripts/eod_report.py
"""
import psycopg2
import os
from datetime import date

DB = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB", "banking_dw"),
    "user":     os.getenv("PG_USER", "banking"),
    "password": os.getenv("PG_PASSWORD", "banking123"),
}

conn = psycopg2.connect(**DB)
cur = conn.cursor()
today = date.today()

cur.execute("""
    SELECT
        COUNT(*)                                              AS total_txns,
        COALESCE(SUM(amount), 0)                             AS total_amount_vnd,
        SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END)          AS fraud_count,
        COUNT(DISTINCT channel)                              AS channels_active,
        COUNT(DISTINCT customer_id)                          AS unique_customers
    FROM fact_txn
    WHERE DATE(loaded_at) = %s
""", (today,))
row = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM fact_alert WHERE DATE(detected_at) = %s", (today,))
alert_count = cur.fetchone()[0]

cur.close()
conn.close()

print(f"{'='*50}")
print(f"  END-OF-DAY REPORT  {today}")
print(f"{'='*50}")
print(f"  Total transactions   : {row[0]:>10,}")
print(f"  Total amount (VND)   : {row[1]:>10,}")
print(f"  Fraud flagged        : {row[2]:>10,}")
print(f"  Fraud alerts         : {alert_count:>10,}")
print(f"  Unique customers     : {row[4]:>10,}")
print(f"  Channels active      : {row[3]:>10}")
print(f"{'='*50}")
