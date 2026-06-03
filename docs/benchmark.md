# Benchmark Results

## Baseline (Python manual ETL)
- Dataset: 10,000 rows — `output/txn_all.csv`
- Script: `python/baseline/baseline_etl.py`
- Method: row-by-row `psycopg2.execute()`, commit every 500 rows (no COPY, no executemany)
- Time: **4.16 seconds**
- Throughput: **2,405 rows/s**
- Errors: 0
- Date measured: 2026-06-04

## NiFi Pipeline
- Dataset: same 10,000 rows
- Time: [ĐO VÀ ĐIỀN] seconds
- Throughput: [ĐO] rows/s
- Improvement vs baseline: [tính %]

## Fraud Detection Latency
- Method: Kafka ingest timestamp → `fact_alert.created_at` in PostgreSQL
- p50: [ĐO] seconds
- p95: [ĐO] seconds
- Target: < 5 seconds

## Error Rate
- Total records processed: [ĐO]
- Dead-letter records: [ĐO]
- Error rate: [tính %]
- Target: < 1%

## Data Masking Verification
- Query: `SELECT COUNT(*) FROM fact_txn WHERE account_masked NOT LIKE '****%'`
- Result: [phải = 0]

## Infrastructure
- Docker stack RAM: [docker stats] GB
- NiFi startup time: ~2 minutes (90s start_period)
- Full stack startup: [ĐO] minutes
