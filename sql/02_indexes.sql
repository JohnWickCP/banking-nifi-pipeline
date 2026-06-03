-- Performance indexes for Banking NiFi Pipeline DW
-- Run after 01_schema.sql

-- fact_txn: most common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_txn_account    ON fact_txn (account_masked);
CREATE INDEX IF NOT EXISTS idx_fact_txn_channel    ON fact_txn (channel);
CREATE INDEX IF NOT EXISTS idx_fact_txn_fraud      ON fact_txn (fraud_flag) WHERE fraud_flag = TRUE;
CREATE INDEX IF NOT EXISTS idx_fact_txn_time       ON fact_txn (time_id);
CREATE INDEX IF NOT EXISTS idx_fact_txn_date       ON fact_txn (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_txn_loaded_at  ON fact_txn (loaded_at);

-- fact_alert: join with fact_txn + filter by severity
CREATE INDEX IF NOT EXISTS idx_fact_alert_txn      ON fact_alert (transaction_id);
CREATE INDEX IF NOT EXISTS idx_fact_alert_severity ON fact_alert (severity);
CREATE INDEX IF NOT EXISTS idx_fact_alert_detected ON fact_alert (detected_at);

-- audit_log: replay + search
CREATE INDEX IF NOT EXISTS idx_audit_txn           ON audit_log (txn_id);
CREATE INDEX IF NOT EXISTS idx_audit_logged        ON audit_log (logged_at);
