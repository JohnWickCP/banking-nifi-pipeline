-- Verification queries for Fraud Rule 4 — Duplicate Detection
-- Run after send_duplicate_test.py completes

-- 1. Check fact_txn: tx1 fraud_flag=false, tx2 fraud_flag=true
SELECT
    transaction_id,
    account_masked,
    amount,
    merchant_id,
    fraud_flag,
    alert_id,
    loaded_at
FROM fact_txn
WHERE transaction_id LIKE 'DUPTEST%'
ORDER BY loaded_at;

-- 2. Check fact_alert: rule_triggered=duplicate, severity=LOW
SELECT
    alert_id,
    transaction_id,
    rule_triggered,
    severity,
    detected_at
FROM fact_alert
WHERE rule_triggered = 'duplicate'
ORDER BY detected_at DESC
LIMIT 5;

-- 3. Cross-join verify: alert_id must match between fact_txn and fact_alert
SELECT
    t.transaction_id,
    t.fraud_flag,
    t.alert_id       AS txn_alert_id,
    a.alert_id       AS alert_alert_id,
    a.rule_triggered,
    a.severity
FROM fact_txn t
JOIN fact_alert a ON t.alert_id = a.alert_id
WHERE t.transaction_id LIKE 'DUPTEST%';

-- 4. Confirm tx1 is clean (no fraud)
SELECT transaction_id, fraud_flag, alert_id
FROM fact_txn
WHERE transaction_id LIKE 'DUPTEST%-001';
