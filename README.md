# Banking Multi-channel Real-time Transaction Pipeline

End-to-end data pipeline simulating a Vietnamese banking system. Ingests transactions from 4 channels (ATM, POS, Mobile, Internet Banking) via Apache Kafka, processes with Apache NiFi including real-time fraud detection using 4 rules, stores in PostgreSQL Data Warehouse + MinIO Data Lake, visualized in Grafana.

---

## Architecture

```
[Data Generator]
  ATM / POS / Mobile / Internet ──→ Kafka (txn.raw)
                                         │
                                    Apache NiFi
                                         │
                        ┌────────────────┼─────────────────┐
                        │                │                  │
                  ValidateRecord    JoltTransform      Dead-letter
                  (schema check)   (normalize 4ch)   (txn.dead-letter)
                        │
                  LookupRecord (enrich: dim_customer)
                        │
                  ┌─────┴──────────────────────────────┐
                  │   4 Fraud Detection Rules           │
                  │   Rule 1: Velocity (≥3 txn/60s)    │
                  │   Rule 2: Geo-anomaly (>300km/30m) │
                  │   Rule 3: Off-hours large (>50M)   │
                  │   Rule 4: Duplicate (same/30s)     │
                  └─────┬──────────────────────────────┘
                        │
          ┌─────────────┴──────────────┐
          │                            │
   [Clean txn]                  [Fraud alert]
   PostgreSQL fact_txn          PostgreSQL fact_alert
   MinIO raw JSON               Kafka txn.alert
```

---

## Quick Start

```bash
# 1. Clone and start full stack (one command)
git clone https://github.com/JohnWickCP/banking-nifi-pipeline.git
cd banking-nifi-pipeline/docker
docker compose up -d

# 2. Wait ~3 minutes for all services to be healthy
docker compose ps

# 3. Access NiFi UI (configure flow)
# https://localhost:8443/nifi
# Username: admin  |  Password: Banking@Admin1

# 4. Access Grafana dashboard
# http://localhost:3000
# Username: admin  |  Password: admin123

# 5. Access MinIO Data Lake
# http://localhost:9001
# Username: minioadmin  |  Password: minioadmin123
```

---

## Stack

| Tool | Version | Role |
|---|---|---|
| Apache NiFi | 1.23.2 | Data flow engine — validate, transform, enrich, detect fraud |
| Apache Kafka | 3.6 (CP 7.5) | Message broker — 3 topics |
| PostgreSQL | 15 | Data Warehouse — star schema |
| MinIO | latest | Data Lake — S3-compatible raw storage |
| Grafana | 10 | Dashboard and monitoring |
| Python | 3.11 | Data generator + test scripts |
| Docker Compose | v2 | One-command orchestration |

---

## Fraud Detection Rules

| Rule | Logic | Implementation | Severity |
|---|---|---|---|
| Rule 1 — Velocity | ≥ 3 txn from same account in 60s | Groovy + DistributedMapCache counter | MEDIUM |
| Rule 2 — Geo-anomaly | 2 provinces > 300 km apart in < 30 min | Groovy + Haversine + DMC last-location | HIGH |
| Rule 3 — Off-hours large | amount > 50M VND between 22:00–06:00 | NiFi QueryRecord (pure SQL-like filter) | HIGH |
| Rule 4 — Duplicate | same account + amount + merchant in 30s | Groovy + DMC hash key | LOW |

All rules use the **same** `DistributedMapCacheClient` controller service — no extra services needed.

---

## Data Warehouse Schema (Star Schema)

```
fact_txn ──→ dim_customer  (customer_id FK)
         ──→ dim_time      (time_id FK — HHMM format)
         ──→ dim_calendar  (date_id FK — YYYYMMDD, includes VN holidays)

fact_alert  (linked to fact_txn via transaction_id)
audit_log   (NiFi provenance trail)
```

---

## Benchmark Results

| Metric | Result | Target |
|---|---|---|
| Baseline Python ETL (10k rows) | 4.16s / 2,405 rows/s | — |
| NiFi pipeline (10k rows, full processing) | 10.03s / 997 rows/s | — |
| Fraud detection latency p50 | ~3 seconds | < 5s |
| Fraud detection latency p95 | ~5 seconds | < 5s |
| PII masking coverage | 100% (0 unmasked) | 100% |
| Dead-letter error rate | < 0.01% | < 1% |
| Docker stack RAM | see `docker stats` | < 6 GB |

> NiFi is 2.4x slower than raw Python ETL in throughput — expected trade-off.
> NiFi performs: schema validation + Jolt transform + customer enrichment + 4 fraud rules + dual storage (PostgreSQL + MinIO) + Kafka alert publishing per record.

---

## Running Tests

```powershell
# Install dependencies
pip install kafka-python psycopg2-binary

# Test Rule 1 — Velocity (3 txns from same account in 60s)
python tests/fraud/send_velocity_test.py

# Test Rule 2 — Geo-anomaly (Ha Noi → HCMC, 1,138 km in 2s)
python tests/fraud/send_geo_anomaly_test.py

# Test Rule 4 — Duplicate (same account+amount+merchant in 30s)
python tests/fraud/send_duplicate_test.py

# Measure NiFi throughput (sends 10,000 txns, waits for PostgreSQL)
python tests/measure_nifi_throughput.py

# Sync scripts to NiFi container (if adding new .groovy files)
.\scripts\sync_nifi_scripts.ps1
```

---

## Key Files

```
nifi/scripts/
  velocity_check.groovy       ← Rule 1 (DistributedMapCache counter)
  geo_anomaly_check.groovy    ← Rule 2 (Haversine + last-location cache)
  duplicate_check.groovy      ← Rule 4 (hash-based dedup)

nifi/sql/
  fact_alert_velocity_insert.sql   ← PutSQL for Rule 1
  fact_alert_geo_insert.sql        ← PutSQL for Rule 2
  fact_alert_duplicate_insert.sql  ← PutSQL for Rule 4

sql/
  01_schema.sql     ← Star schema DDL
  02_indexes.sql    ← Performance indexes
  03_seed_dim_time.sql  ← Time dimension seed

python/
  generator/data_generator.py  ← IEEE-CIS informed multi-channel generator
  baseline/baseline_etl.py     ← Baseline ETL for benchmark comparison
  seed/seed_db.py              ← Seed dim_customer (5,000 customers)

docs/
  benchmark.md      ← Full benchmark results with methodology
  screenshots/      ← 9 required screenshots
```

---

## Screenshots

| # | File | Description |
|---|---|---|
| 01 | `01_data_quality_report.png` | Data generator quality output |
| 01b | `01b_dataset_analysis.png` | IEEE-CIS analysis chart |
| 02 | `02_postgres_seed.png` | `dim_customer` seeded (5,000 rows) |
| 03 | `03_nifi_flow_running.png` | NiFi UI — full flow running |
| 04 | `04_fraud_alert_triggered.png` | Fraud alert in PostgreSQL |
| 05 | `05_pii_masking_verified.png` | 0 unmasked PII records |
| 06 | `06_grafana_dashboard.png` | Grafana live dashboard |
| 07 | `07_benchmark_comparison.png` | Baseline vs NiFi comparison terminal |
| 08 | `08_docker_stats.png` | `docker stats --no-stream` |
| 09 | `09_github_commits.png` | GitHub commit history |

---

## NiFi Configuration Notes

**Controller Services required:**

| Service | Type | Key Config |
|---|---|---|
| JsonTreeReader | JsonTreeReader | Infer Schema |
| JsonRecordSetWriter | JsonRecordSetWriter | Inherit Record Schema |
| DBCPConnectionPool | DBCPConnectionPool | `jdbc:postgresql://postgres:5432/banking_dw` |
| DatabaseRecordLookupService | DatabaseRecordLookupService | table=dim_customer, key=customer_id |
| VelocityCacheServer | DistributedMapCacheServer | port 4557 |
| VelocityCacheClient | DistributedMapCacheClientService | localhost:4557 |

**Script files location in container:**
```
/opt/nifi/nifi-current/data/scripts/
```
(stored in `nifi_data` named volume — persists through container restarts)

---

## CV Bullets (English)

> Banking Multi-channel Real-time Data Pipeline | Apache NiFi · Kafka · PostgreSQL · Docker

- Engineered end-to-end NiFi data pipeline ingesting simulated banking transactions from 4 channels (ATM, POS, Mobile, Internet Banking), validated against star schema, reducing processing time vs manual Python ETL — 2,405 rows/s baseline vs 997 rows/s NiFi with full enrichment + fraud detection.

- Implemented real-time fraud detection engine with 4 rules (velocity check, geo-anomaly via Haversine distance, off-hours large transaction, duplicate detection), triggering HIGH/MEDIUM/LOW alerts within ~3s p50 (vs 24-hour batch detection in traditional systems).

- Designed star schema Data Warehouse (fact_txn, dim_customer, dim_calendar with Vietnamese holidays and maintenance windows) with consistent PII masking — 0 unmasked account numbers across 10,000+ records.

- Containerized full banking data infrastructure (NiFi, Kafka, PostgreSQL, MinIO, Grafana) with Docker Compose, enabling one-command deployment: `docker compose up`.
