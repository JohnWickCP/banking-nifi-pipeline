"""
send_geo_anomaly_test.py — Test Rule 2: Geo-anomaly detection
Logic: same account appears in 2 provinces >300km apart within 30 minutes.

Scenario:
  Txn 1: account ****9999 at Ha Noi (21.03°N, 105.83°E)
  Txn 2: account ****9999 at Ho Chi Minh (10.82°N, 106.63°E)  — ~1,750 km apart
  Both within 30s → should trigger ALT-G-* alert

Usage:
    python tests/fraud/send_geo_anomaly_test.py
"""

import json
import time
import uuid
import sys
from datetime import datetime

try:
    from kafka import KafkaProducer
except ImportError:
    print("ERROR: pip install kafka-python")
    sys.exit(1)

BROKER = "localhost:9092"
TOPIC  = "txn.raw"

RUN_ID = uuid.uuid4().hex[:8].upper()

# Ha Noi coordinates
HANOI_LAT, HANOI_LON = 21.0278, 105.8342
# Ho Chi Minh City coordinates (~1,750 km from Ha Noi)
HCMC_LAT,  HCMC_LON  = 10.8231, 106.6297

TXNS = [
    {
        "transaction_id":    f"GEOTEST-{RUN_ID}-001",
        "timestamp":         datetime.utcnow().isoformat(timespec="seconds"),
        "account_masked":    "****9999",
        "customer_id":       "CUS000099",
        "channel":           "ATM",
        "merchant_id":       "MER09901",
        "merchant_province": "Ha Noi",
        "merchant_lat":      HANOI_LAT,
        "merchant_lon":      HANOI_LON,
        "amount":            3_000_000,
        "currency":          "VND",
        "status":            "SUCCESS",
        "fraud_label":       0,
        "fraud_type":        None,
    },
    {
        "transaction_id":    f"GEOTEST-{RUN_ID}-002",
        "timestamp":         datetime.utcnow().isoformat(timespec="seconds"),
        "account_masked":    "****9999",
        "customer_id":       "CUS000099",
        "channel":           "POS",
        "merchant_id":       "MER09902",
        "merchant_province": "Ho Chi Minh",
        "merchant_lat":      HCMC_LAT,
        "merchant_lon":      HCMC_LON,
        "amount":            2_000_000,
        "currency":          "VND",
        "status":            "SUCCESS",
        "fraud_label":       1,
        "fraud_type":        "geo_anomaly",
    },
]

def haversine_km(lat1, lon1, lat2, lon2):
    import math
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def main():
    dist = haversine_km(HANOI_LAT, HANOI_LON, HCMC_LAT, HCMC_LON)
    print(f"[geo-anomaly-test] Run ID: {RUN_ID}")
    print(f"  Ha Noi → HCMC distance: {dist:.0f} km  (threshold: 300 km)")
    print(f"  Account: ****9999 | Window: 30 minutes")
    print()

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for i, txn in enumerate(TXNS, 1):
        producer.send(TOPIC, txn)
        producer.flush()
        loc = f"{txn['merchant_province']} ({txn['merchant_lat']:.4f}, {txn['merchant_lon']:.4f})"
        print(f"  Sent txn {i}: {txn['transaction_id']} @ {loc}")
        if i < len(TXNS):
            time.sleep(2)   # 2s gap — well within 30-minute window

    producer.close()

    print()
    print("  Wait 5s for NiFi to process...")
    time.sleep(5)
    print()
    print("  Verify results:")
    print("    docker exec banking-postgres psql -U banking -d banking_dw -c \\")
    print(f"      \"SELECT transaction_id, fraud_flag, alert_id FROM fact_txn WHERE transaction_id LIKE 'GEOTEST-{RUN_ID}%' ORDER BY loaded_at;\"")
    print()
    print("    docker exec banking-postgres psql -U banking -d banking_dw -c \\")
    print("      \"SELECT alert_id, transaction_id, rule_triggered, severity, detected_at FROM fact_alert WHERE rule_triggered='geo_anomaly' ORDER BY detected_at DESC LIMIT 5;\"")
    print()
    print("  Expected:")
    print(f"    GEOTEST-{RUN_ID}-001 | fraud_flag=false | (clean — first location)")
    print(f"    GEOTEST-{RUN_ID}-002 | fraud_flag=true  | ALT-G-xxxxxxxx  ← geo anomaly")

if __name__ == "__main__":
    main()
