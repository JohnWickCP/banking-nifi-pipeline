-- Verify Fraud Rule 1 (Velocity Check) results
-- Run after send_velocity_test.py or manual Kafka injection

-- 1. Check fact_txn: tx3 must have fraud_flag=true and alert_id populated
SELECT
    transaction_id,
    account_masked,
    amount,
    fraud_flag,
    alert_id,
    loaded_at
FROM fact_txn
WHERE transaction_id LIKE 'VELTEST%'
   OR transaction_id LIKE 'V2%'
ORDER BY loaded_at DESC
LIMIT 10;

-- 2. Check fact_alert: must have 1 velocity row per test run
SELECT
    alert_id,
    transaction_id,
    rule_triggered,
    severity,
    detected_at
FROM fact_alert
WHERE rule_triggered = 'velocity'
ORDER BY detected_at DESC
LIMIT 5;

-- 3. Cross-check: alert_id in fact_txn must match fact_alert
SELECT
    t.transaction_id,
    t.fraud_flag,
    t.alert_id          AS txn_alert_id,
    a.alert_id          AS alert_table_id,
    a.rule_triggered,
    a.severity
FROM fact_txn t
JOIN fact_alert a ON t.alert_id = a.alert_id
WHERE a.rule_triggered = 'velocity'
ORDER BY a.detected_at DESC
LIMIT 5;
