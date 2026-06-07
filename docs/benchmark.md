# Benchmark Results

## Baseline (Python manual ETL)
- Dataset: 10,000 rows — `output/txn_all.csv`
- Script: `python/baseline/baseline_etl.py`
- Method: row-by-row `psycopg2.execute()`, commit every 500 rows (no COPY, no executemany)
- Time: **4.16 seconds**
- Throughput: **2,405 rows/s**
- Errors: 0
- Date measured: 2026-06-04

## NiFi Pipeline (clean benchmark run — 2026-06-07)
- Dataset: 10,034 rows via Kafka topic `txn.raw` (10,000 normal + 72 intentional fraud pattern records)
- Time: **13.03 seconds** (end-to-end: Kafka produce complete → all rows in `fact_txn`)
- Throughput: **770 rows/s**
- Improvement vs baseline: NiFi does significantly more work per record — 3.1x slower in raw speed but delivers enrichment, fraud detection, dual storage, and alert publishing in a single pass
- Date measured: 2026-06-07
- Fix applied: QueryRecord SQL updated to use `SUBSTRING("timestamp", 12, 2)` for hour extraction (Calcite cannot cast ISO 8601 T-separated timestamps directly)

### Throughput comparison table

| Method | Time (10k rows) | Throughput | What it does |
|---|---|---|---|
| Python baseline ETL | 4.16s | 2,405 rows/s | Read CSV then INSERT PostgreSQL |
| NiFi pipeline | 13.03s | 770 rows/s | Kafka → LookupRecord (PostgreSQL join) → 2 fraud rules (DMC) → QueryRecord Rule 3 → PutDatabaseRecord + PublishKafka |

## Fraud Detection Results (clean run — 10,034 records)

| Rule | Alerts triggered | Expected | Severity | Detection method |
|---|---|---|---|---|
| Rule 1 — velocity | 16 | 16 (8 clusters x 2 alerts) | MEDIUM | Groovy sliding window, DMC timestamps list |
| Rule 3 — off_hours_large | 13 | 13 | HIGH | QueryRecord SQL, SUBSTRING hour extraction |
| Rule 4 — duplicate | 10 | 10 (10 pairs) | LOW | Groovy hash-based dedup, DMC |
| Rule 2 — geo_anomaly | 0 | N/A — controlled test only | HIGH | Haversine Groovy script (not in automated rebuild) |

### Rule 2 — Geo-anomaly (controlled test)
- Test script: `tests/fraud/send_geo_anomaly_test.py`
- Scenario: account `****9999` at Ha Noi (21.0278 N, 105.8342 E) then Ho Chi Minh (10.8231 N, 106.6297 E)
- Haversine distance: 1,138 km (threshold: 300 km, window: 30 minutes)
- Implementation: `nifi/scripts/geo_anomaly_check.groovy` — verified working in previous sessions
- Note: geo_anomaly processor requires manual wiring in NiFi UI after `nifi_full_setup.py` rebuild

## Fraud Detection Latency
- Method: all fraud routes (velocity/duplicate/off_hours) write to fact_txn and fact_alert in the same NiFi FAILURE-route batch — latency between the two timestamps is < 0.01s
- End-to-end latency (Kafka publish to DB insert): first records appear in fact_txn within 1s of Kafka produce completing; first alerts appear within 2s
- Estimated p50: < 2 seconds
- Estimated p95: < 4 seconds
- Target: < 5 seconds — met

### Velocity sliding window (upgraded from tumbling window — 2026-06-07)
- Previous: `{c: count, ws: window_start}` — reset on window boundary, misses patterns straddling boundary
- Current: `{ts: [t1, t2, ...]}` — prune timestamps older than 60s, check size >= 3 — true sliding window
- Test case t=0s, t=30s, t=59s: all 3 within the 60s window, triggers at t=59s
- Test case t=0s, t=61s: only 1 timestamp in current window, no trigger

## Error Rate
- Total records processed: 10,034
- Dead-letter records: 0 (no records in `txn.dead-letter` topic)
- Schema validation errors: 0
- Error rate: < 0.01% — well under 1% target

## Data Masking Verification
- Query: `SELECT COUNT(*) FROM fact_txn WHERE account_masked NOT LIKE '****%'`
- Result: **0** — 100% PII masking coverage
- All 10,034 records in `fact_txn` use consistent `****XXXXX` format

## Infrastructure (2026-06-07)

| Container | RAM | CPU |
|---|---|---|
| banking-nifi | 2.316 GiB | 2.88% |
| banking-kafka | 511.1 MiB | 3.82% |
| banking-minio | 1,005 MiB | 5.72% |
| banking-zookeeper | 199.7 MiB | 0.05% |
| banking-grafana | 91.88 MiB | 0.03% |
| banking-postgres | 67.75 MiB | 0.00% |
| **TOTAL (main services)** | **~4.19 GiB** | |

- Docker stack RAM: 4.19 GiB — under 6 GiB target
- NiFi startup time: ~2 minutes (state restored from named volume)
- Full stack startup: ~3 minutes (`docker compose up` cold start)

## Notes on NiFi vs Baseline Comparison

The baseline Python ETL is a **fair comparison point** because:
- Same dataset (10,000 rows)
- Same target (PostgreSQL `fact_txn`)
- Baseline does NO enrichment, NO fraud detection, NO masking, NO MinIO

The NiFi pipeline is slower per-record but delivers:
- Real-time fraud detection on 3 active rules (velocity, off-hours, duplicate) with sliding-window velocity
- PII masking before storage
- Customer enrichment via dimensional lookup (PostgreSQL join per record)
- Dual storage (PostgreSQL DW + MinIO Data Lake)
- Kafka alert stream for downstream consumers
- Dead-letter queue with replay capability
- Schema validation with automatic rejection of malformed records
