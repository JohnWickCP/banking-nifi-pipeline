-- Verify Rule 2 geo-anomaly test results
-- Run after: python tests/fraud/send_geo_anomaly_test.py

-- 1. Check fact_txn: txn 001 clean, txn 002 fraud
SELECT
    transaction_id,
    merchant_province,
    fraud_flag,
    alert_id,
    loaded_at
FROM fact_txn
WHERE transaction_id LIKE 'GEOTEST-%'
ORDER BY loaded_at DESC
LIMIT 10;

-- 2. Check fact_alert: should have 1 geo_anomaly alert (HIGH severity)
SELECT
    alert_id,
    transaction_id,
    rule_triggered,
    severity,
    detected_at
FROM fact_alert
WHERE rule_triggered = 'geo_anomaly'
ORDER BY detected_at DESC
LIMIT 5;

-- 3. Summary across all fraud rules
SELECT
    rule_triggered,
    severity,
    COUNT(*) AS alert_count
FROM fact_alert
GROUP BY rule_triggered, severity
ORDER BY alert_count DESC;
