-- Star schema for Banking NiFi Pipeline Data Warehouse
-- Auto-run by PostgreSQL on first volume init (docker-entrypoint-initdb.d)

-- ── Dimension: Customer ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     VARCHAR(10) PRIMARY KEY,
    account_masked  VARCHAR(20),
    segment         VARCHAR(20),        -- VIP/Premium/Standard/New/Watch
    risk_score      INTEGER,
    home_province   VARCHAR(50),
    home_lat        DECIMAL(9,6),
    home_lon        DECIMAL(9,6)
);

-- ── Dimension: Time (1 row per minute, 1440 rows) ─────────────────────────
CREATE TABLE IF NOT EXISTS dim_time (
    time_id          INTEGER PRIMARY KEY,   -- HHMM: 0930
    hour             INTEGER,
    minute           INTEGER,
    is_peak_hour     BOOLEAN,               -- 8-11h, 13-16h
    is_business_hour BOOLEAN,               -- 8-17h
    period_label     VARCHAR(20)            -- morning/afternoon/evening/night
);

-- ── Dimension: Calendar ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_calendar (
    date_id         INTEGER PRIMARY KEY,    -- YYYYMMDD
    full_date       DATE UNIQUE,
    day_of_week     INTEGER,                -- 0=Mon … 6=Sun (Python weekday)
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE,
    holiday_name    VARCHAR(100),
    is_maintenance  BOOLEAN DEFAULT FALSE   -- Sunday maintenance window
);

-- ── Fact: Transactions ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_txn (
    id                BIGSERIAL PRIMARY KEY,
    transaction_id    VARCHAR(20) UNIQUE NOT NULL,
    account_masked    VARCHAR(20) NOT NULL,
    customer_id       VARCHAR(10),
    channel           VARCHAR(20),           -- ATM/POS/mobile/internet
    merchant_id       VARCHAR(10),
    merchant_province VARCHAR(50),
    amount            BIGINT NOT NULL,        -- VND
    currency          CHAR(3) DEFAULT 'VND',
    status            VARCHAR(20),
    fraud_flag        BOOLEAN DEFAULT FALSE,
    alert_id          VARCHAR(20),
    time_id           INTEGER,               -- FK → dim_time
    date_id           INTEGER,               -- FK → dim_calendar
    loaded_at         TIMESTAMP DEFAULT NOW()
);

-- ── Fact: Alerts ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_alert (
    id             BIGSERIAL PRIMARY KEY,
    alert_id       VARCHAR(20) UNIQUE NOT NULL,
    transaction_id VARCHAR(20),
    rule_triggered VARCHAR(50),              -- velocity/geo_anomaly/off_hours/duplicate
    severity       VARCHAR(10),             -- LOW/MEDIUM/HIGH
    detected_at    TIMESTAMP NOT NULL,
    resolved_at    TIMESTAMP,
    created_at     TIMESTAMP DEFAULT NOW()
);

-- ── Audit Log ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
    id             BIGSERIAL PRIMARY KEY,
    flowfile_uuid  VARCHAR(50),
    processor_name VARCHAR(100),
    event_type     VARCHAR(50),
    txn_id         VARCHAR(20),
    logged_at      TIMESTAMP DEFAULT NOW()
);

-- ── Baseline staging (used by baseline_etl.py for benchmark) ──────────────
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
