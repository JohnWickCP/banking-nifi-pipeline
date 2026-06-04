-- PutSQL statement for CheckGeoAnomaly_Rule2 (failure) → fact_alert
-- FlowFile attributes used: alert_id, txn_id, rule_triggered, severity
-- ON CONFLICT DO NOTHING: idempotent — safe for dead-letter replay
INSERT INTO fact_alert (alert_id, transaction_id, rule_triggered, severity, detected_at)
VALUES (
    '${alert_id}',
    '${txn_id}',
    '${rule_triggered}',
    '${severity}',
    NOW()
)
ON CONFLICT (alert_id) DO NOTHING
