"""
continuous_load.py — Chạy liên tục gửi transaction vào pipeline + fraud patterns
Usage: python tests/continuous_load.py [--hours 2] [--rate 10]
  --hours : số giờ chạy (default 2)
  --rate  : số txn/giây (default 10)
"""
import json, random, time, argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC  = "txn.raw"

PROVINCES = [
    ("Ha Noi", 21.0278, 105.8342),
    ("Ho Chi Minh", 10.8231, 106.6297),
    ("Da Nang", 16.0544, 108.2022),
    ("Can Tho", 10.0452, 105.7469),
    ("Hai Phong", 20.8449, 106.6881),
    ("Hue", 16.4637, 107.5909),
    ("Nha Trang", 12.2388, 109.1967),
]
CHANNELS = ["ATM", "POS", "mobile", "internet"]

txn_counter = 0

def make_txn(account=None, amount=None, merchant=None, province=None, fraud_label=0, fraud_type=None):
    global txn_counter
    txn_counter += 1
    prov = province or random.choice(PROVINCES)
    acct = account or f"****{random.randint(1000,9999):04d}"
    return {
        "transaction_id":    f"LOAD-{txn_counter:09d}",
        "timestamp":         datetime.utcnow().isoformat(timespec="seconds"),
        "account_masked":    acct,
        "customer_id":       f"CUS{random.randint(1,5000):06d}",
        "channel":           random.choice(CHANNELS),
        "merchant_id":       merchant or f"MER{random.randint(1,1000):05d}",
        "merchant_province": prov[0],
        "merchant_lat":      prov[1] + random.uniform(-0.05, 0.05),
        "merchant_lon":      prov[2] + random.uniform(-0.05, 0.05),
        "amount":            amount or random.choice([500_000, 1_000_000, 2_000_000, 5_000_000,
                                                      10_000_000, 20_000_000]),
        "currency":          "VND",
        "status":            random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[90, 7, 3])[0],
        "fraud_label":       fraud_label,
        "fraud_type":        fraud_type,
    }

def send_velocity_cluster(producer, n=4):
    """Rule 1: 4 txns cùng account trong 5 giây"""
    acct = f"****{random.randint(1000,9999):04d}"
    for _ in range(n):
        producer.send(TOPIC, make_txn(account=acct, fraud_label=1, fraud_type="velocity"))
        time.sleep(0.2)
    return acct

def send_off_hours_large(producer):
    """Rule 3: amount > 50M"""
    producer.send(TOPIC, make_txn(
        amount=random.randint(51_000_000, 200_000_000),
        fraud_label=1, fraud_type="off_hours_large"
    ))

def send_duplicate(producer):
    """Rule 4: cùng account+amount+merchant trong 30s"""
    acct = f"****{random.randint(1000,9999):04d}"
    amt  = random.choice([2_000_000, 5_000_000, 10_000_000])
    mer  = f"MER{random.randint(1,1000):05d}"
    for _ in range(2):
        producer.send(TOPIC, make_txn(account=acct, amount=amt, merchant=mer,
                                      fraud_label=1, fraud_type="duplicate"))
        time.sleep(5)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=float, default=2.0)
    parser.add_argument("--rate",  type=int,   default=10)
    args = parser.parse_args()

    duration_s = args.hours * 3600
    interval   = 1.0 / args.rate

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        batch_size=65536,
    )

    start = time.time()
    next_velocity  = start + 300     # fraud cluster mỗi 5 phút
    next_ofhours   = start + 600     # off-hours large mỗi 10 phút
    next_duplicate = start + 180     # duplicate mỗi 3 phút
    next_report    = start + 60      # progress mỗi 1 phút
    fraud_count    = 0

    print(f"[continuous_load] Starting — {args.hours}h @ {args.rate} txn/s")
    print(f"  Target: ~{int(args.hours * 3600 * args.rate):,} transactions")
    print(f"  Fraud patterns: velocity every 5min, duplicate every 3min, off-hours every 10min")
    print()

    while True:
        now = time.time()
        elapsed = now - start
        if elapsed >= duration_s:
            break

        # Normal transaction
        producer.send(TOPIC, make_txn())
        time.sleep(interval)

        # Fraud patterns
        if now >= next_velocity:
            acct = send_velocity_cluster(producer)
            fraud_count += 4
            print(f"  [fraud] velocity cluster → account {acct}")
            next_velocity = now + 300

        if now >= next_ofhours:
            send_off_hours_large(producer)
            fraud_count += 1
            print(f"  [fraud] off-hours large txn sent")
            next_ofhours = now + 600

        if now >= next_duplicate:
            send_duplicate(producer)
            fraud_count += 2
            print(f"  [fraud] duplicate pair sent")
            next_duplicate = now + 180

        # Progress report
        if now >= next_report:
            rate = txn_counter / elapsed
            remaining = timedelta(seconds=int(duration_s - elapsed))
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                  f"sent={txn_counter:,}  fraud={fraud_count}  "
                  f"rate={rate:.1f}/s  remaining={remaining}")
            next_report = now + 60

    producer.flush()
    producer.close()
    elapsed = time.time() - start
    print()
    print("=" * 55)
    print(f"  DONE — {txn_counter:,} transactions in {elapsed/60:.1f} minutes")
    print(f"  Fraud patterns sent: {fraud_count}")
    print(f"  Avg rate: {txn_counter/elapsed:.1f} txn/s")
    print("=" * 55)

if __name__ == "__main__":
    main()
