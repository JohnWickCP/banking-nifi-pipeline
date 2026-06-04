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
- Dataset: same 10,000 rows via Kafka topic `txn.raw`
- Time: **10.03 seconds** (end-to-end: Kafka produce → all rows in `fact_txn`)
- Throughput: **997 rows/s**
- Improvement vs baseline: NiFi is 2.4x slower in raw throughput — expected trade-off
- **NiFi does significantly more work per record**: ValidateRecord → JoltTransformJSON → LookupRecord (PostgreSQL join) → 4 fraud rules (3× DMC cache ops) → PutDatabaseRecord + PutS3Object + PublishKafka
- Date measured: 2026-06-04

### Throughput comparison table

| Method | Time (10k rows) | Throughput | What it does |
|---|---|---|---|
| Python baseline ETL | 4.16s | 2,405 rows/s | Read CSV → INSERT PostgreSQL |
| NiFi pipeline | 10.03s | 997 rows/s | Kafka → Validate → Jolt → Enrich → 4 fraud rules → PostgreSQL + MinIO + Kafka alert |

## Fraud Detection Latency
- Method: message sent to Kafka → alert written to `fact_alert` (observed wall-clock time in tests)
- p50: **~3 seconds**
- p95: **~5 seconds**
- Max observed: < 10 seconds (including pipeline startup delay)
- Target: < 5 seconds ✅ (p50 meets target; p95 at boundary)

### Fraud rules triggered (cumulative, all test runs)

| Rule | Alerts generated | Severity |
|---|---|---|
| velocity (Rule 1) | 18,006 | MEDIUM |
| duplicate (Rule 4) | 1,004 | LOW |
| geo_anomaly (Rule 2) | 1 | HIGH |
| off_hours_large (Rule 3) | (via QueryRecord routing) | HIGH |

## Error Rate
- Total records processed through NiFi: ~30,000+ (multiple test batches)
- Dead-letter records: **0** (no records in `txn.dead-letter` topic)
- Schema validation errors: 0 (all test data conforms to schema)
- Error rate: **< 0.01%** ✅ (well under 1% target)

## Data Masking Verification
- Query: `SELECT COUNT(*) FROM fact_txn WHERE account_masked NOT LIKE '****%'`
- Result: **0** ✅ — 100% PII masking coverage
- All 10,034 records in `fact_txn` use consistent `****XXXX` format

## Infrastructure

| Container | RAM | CPU |
|---|---|---|
| banking-nifi | 2.735 GiB | 2.77% |
| banking-kafka | 763 MiB | 0.54% |
| banking-zookeeper | 144 MiB | 0.08% |
| banking-minio | 91 MiB | 0.08% |
| banking-grafana | 79 MiB | 0.14% |
| banking-postgres | 57 MiB | 0.00% |
| **TOTAL** | **~3.87 GiB** | **~3.6%** |

- Docker stack RAM: **3.87 GiB** ✅ (target < 6 GiB)
- NiFi startup time: ~2 minutes (90s start_period configured)
- Full stack startup: ~3 minutes (`docker compose up` cold start)

## Notes on NiFi vs Baseline Comparison

The baseline Python ETL is a **fair comparison point** because:
- Same dataset (10,000 rows)
- Same target (PostgreSQL `fact_txn`)
- Baseline does NO enrichment, NO fraud detection, NO masking, NO MinIO

The NiFi pipeline is slower per-record but delivers:
- Real-time fraud detection on 4 rules (velocity, geo-anomaly, off-hours, duplicate)
- PII masking before storage
- Customer enrichment via dimensional lookup
- Dual storage (PostgreSQL DW + MinIO Data Lake)
- Kafka alert stream for downstream consumers
- Dead-letter queue with replay capability
- Schema validation with automatic rejection of malformed records
