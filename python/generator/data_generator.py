import random
import json
import hashlib
import csv
import os
from datetime import datetime, timedelta, date
from typing import Optional
import numpy as np

try:
    from faker import Faker
    import pandas as pd
    from tqdm import tqdm
    print("OK: All libraries loaded.")
except ImportError as e:
    print(f"Run: pip install faker pandas numpy tqdm\nError: {e}")
    raise

# ─── CONFIG ──────────────────────────────────────────────────────────────────

CONFIG = {
    "total_transactions": 50_000,   # tổng txn / ngày
    "fraud_rate": 0.02,             # 2% fraud (sát thực tế ngân hàng VN)
    "num_customers": 5_000,         # số khách hàng unique
    "num_merchants": 500,           # số merchant
    "simulation_days": 1,           # số ngày simulate
    "output_dir": "output",         # thư mục output
    "seed": 42,                     # reproducible
}

# Distribution theo giờ trong ngày (banking VN thực tế)
HOURLY_WEIGHT = {
    0: 0.2,  1: 0.1,  2: 0.05, 3: 0.05, 4: 0.1,  5: 0.3,
    6: 0.8,  7: 1.5,  8: 3.2,  9: 4.5,  10: 5.0, 11: 5.5,
    12: 3.0, 13: 4.0, 14: 5.2, 15: 5.8, 16: 6.0, 17: 4.5,
    18: 3.0, 19: 2.5, 20: 2.0, 21: 1.5, 22: 0.8, 23: 0.5,
}

# Distribution theo kênh
CHANNEL_CONFIG = {
    "ATM":      {"weight": 0.25, "format": "csv",  "avg_amount": 2_000_000,  "std": 1_500_000},
    "POS":      {"weight": 0.35, "format": "csv",  "avg_amount": 500_000,    "std": 800_000},
    "mobile":   {"weight": 0.30, "format": "json", "avg_amount": 1_200_000,  "std": 2_000_000},
    "internet": {"weight": 0.10, "format": "json", "avg_amount": 5_000_000,  "std": 8_000_000},
}

# Tỉnh thành VN với tọa độ (để tính geo-anomaly)
VN_LOCATIONS = [
    {"province": "Ha Noi",       "lat": 21.0278, "lon": 105.8342, "weight": 0.25},
    {"province": "Ho Chi Minh",  "lat": 10.8231, "lon": 106.6297, "weight": 0.25},
    {"province": "Da Nang",      "lat": 16.0544, "lon": 108.2022, "weight": 0.10},
    {"province": "Hai Phong",    "lat": 20.8449, "lon": 106.6881, "weight": 0.07},
    {"province": "Can Tho",      "lat": 10.0452, "lon": 105.7469, "weight": 0.06},
    {"province": "Bien Hoa",     "lat": 10.9574, "lon": 106.8426, "weight": 0.05},
    {"province": "Hue",          "lat": 16.4637, "lon": 107.5909, "weight": 0.04},
    {"province": "Nha Trang",    "lat": 12.2388, "lon": 109.1967, "weight": 0.04},
    {"province": "Vung Tau",     "lat": 10.3460, "lon": 107.0843, "weight": 0.04},
    {"province": "Quy Nhon",     "lat": 13.7830, "lon": 109.2196, "weight": 0.03},
    {"province": "Buon Ma Thuot","lat": 12.6667, "lon": 108.0500, "weight": 0.03},
    {"province": "Thai Nguyen",  "lat": 21.5928, "lon": 105.8442, "weight": 0.02},
    {"province": "Nam Dinh",     "lat": 20.4389, "lon": 106.1621, "weight": 0.02},
]

# Customer risk segments
CUSTOMER_SEGMENTS = {
    "VIP":      {"weight": 0.05, "risk_score_range": (10, 30),  "avg_txn_mult": 5.0},
    "Premium":  {"weight": 0.15, "risk_score_range": (10, 40),  "avg_txn_mult": 2.5},
    "Standard": {"weight": 0.60, "risk_score_range": (20, 60),  "avg_txn_mult": 1.0},
    "New":      {"weight": 0.15, "risk_score_range": (30, 80),  "avg_txn_mult": 0.6},
    "Watch":    {"weight": 0.05, "risk_score_range": (60, 100), "avg_txn_mult": 0.8},
}

# Ngày lễ Việt Nam 2025
VN_HOLIDAYS_2025 = [
    date(2025, 1, 1),   # Tết Dương lịch
    date(2025, 1, 28),  # Tết Nguyên Đán nghỉ bù
    date(2025, 1, 29),  date(2025, 1, 30), date(2025, 1, 31),
    date(2025, 2, 1),   date(2025, 2, 2),  date(2025, 2, 3),
    date(2025, 4, 7),   # Giỗ Tổ Hùng Vương
    date(2025, 4, 30),  # Giải phóng miền Nam
    date(2025, 5, 1),   # Quốc tế lao động
    date(2025, 9, 2),   # Quốc khánh
]

# Maintenance windows (1h–3h sáng mỗi Chủ nhật)
def is_maintenance_window(dt: datetime) -> bool:
    return dt.weekday() == 6 and 1 <= dt.hour <= 3

def is_peak_hour(dt: datetime) -> bool:
    return dt.hour in range(8, 12) or dt.hour in range(13, 17)

def is_business_hour(dt: datetime) -> bool:
    return dt.weekday() < 5 and 8 <= dt.hour <= 17

# ─── CONSISTENT MASKING ──────────────────────────────────────────────────────

def mask_account(account_number: str) -> str:
    return "****" + account_number[-4:]

def mask_phone(phone: str) -> str:
    digits = ''.join(filter(str.isdigit, phone))
    return digits[:3] + "****" + digits[-3:]

def mask_national_id(nid: str) -> str:
    return nid[:3] + "******" + nid[-3:]

# ─── CUSTOMER & MERCHANT GENERATION ─────────────────────────────────────────

def generate_customers(n: int, fake: Faker) -> list[dict]:
    customers = []
    segments = list(CUSTOMER_SEGMENTS.keys())
    seg_weights = [CUSTOMER_SEGMENTS[s]["weight"] for s in segments]
    loc_weights = [l["weight"] for l in VN_LOCATIONS]

    for i in range(n):
        segment = random.choices(segments, weights=seg_weights)[0]
        seg_cfg = CUSTOMER_SEGMENTS[segment]
        location = random.choices(VN_LOCATIONS, weights=loc_weights)[0]
        account = f"VCB{random.randint(10**9, 10**10-1)}"
        phone = f"0{random.choice([3,5,7,8,9])}{random.randint(10**7, 10**8-1)}"
        nid = str(random.randint(10**11, 10**12-1))

        customers.append({
            "customer_id":       f"CUS{i+1:06d}",
            "account_number":    account,
            "account_masked":    mask_account(account),
            "phone":             phone,
            "phone_masked":      mask_phone(phone),
            "national_id":       nid,
            "national_id_masked":mask_national_id(nid),
            "full_name":         fake.name(),
            "segment":           segment,
            "risk_score":        random.randint(*seg_cfg["risk_score_range"]),
            "home_province":     location["province"],
            "home_lat":          location["lat"],
            "home_lon":          location["lon"],
            "avg_txn_multiplier":seg_cfg["avg_txn_mult"],
        })
    return customers

def generate_merchants(n: int, fake: Faker) -> list[dict]:
    categories = ["grocery", "restaurant", "fuel", "electronics",
                  "clothing", "pharmacy", "entertainment", "travel",
                  "utilities", "online_shopping"]
    merchants = []
    loc_weights = [l["weight"] for l in VN_LOCATIONS]
    for i in range(n):
        location = random.choices(VN_LOCATIONS, weights=loc_weights)[0]
        merchants.append({
            "merchant_id":       f"MER{i+1:05d}",
            "merchant_name":     fake.company(),
            "category":          random.choice(categories),
            "province":          location["province"],
            "lat":               location["lat"] + random.uniform(-0.5, 0.5),
            "lon":               location["lon"] + random.uniform(-0.5, 0.5),
        })
    return merchants

# ─── TIMESTAMP GENERATION ────────────────────────────────────────────────────

def generate_timestamps(n: int, base_date: date) -> list[datetime]:
    hours = list(HOURLY_WEIGHT.keys())
    weights = list(HOURLY_WEIGHT.values())
    timestamps = []
    for _ in range(n):
        hour = random.choices(hours, weights=weights)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        ts = datetime(base_date.year, base_date.month, base_date.day,
                      hour, minute, second)
        timestamps.append(ts)
    return sorted(timestamps)

# ─── NORMAL TRANSACTION GENERATION ──────────────────────────────────────────

def generate_normal_txn(txn_id: str, ts: datetime, customer: dict,
                        merchant: dict, channel: str, cfg: dict) -> dict:
    amount_raw = abs(np.random.normal(
        cfg["avg_amount"] * customer["avg_txn_multiplier"],
        cfg["std"]
    ))
    amount = max(10_000, round(amount_raw / 1000) * 1000)  # làm tròn 1k

    return {
        "transaction_id":  txn_id,
        "timestamp":       ts.isoformat(),
        "account_number":  customer["account_number"],
        "account_masked":  customer["account_masked"],
        "customer_id":     customer["customer_id"],
        "customer_segment":customer["segment"],
        "channel":         channel,
        "merchant_id":     merchant["merchant_id"],
        "merchant_name":   merchant["merchant_name"],
        "merchant_category":merchant["category"],
        "merchant_province":merchant["province"],
        "merchant_lat":    round(merchant["lat"], 6),
        "merchant_lon":    round(merchant["lon"], 6),
        "amount":          amount,
        "currency":        "VND",
        "status":          "SUCCESS",
        "fraud_label":     0,
        "fraud_type":      None,
        "is_peak_hour":    is_peak_hour(ts),
        "is_business_hour":is_business_hour(ts),
        "is_holiday":      ts.date() in VN_HOLIDAYS_2025,
        "is_maintenance":  is_maintenance_window(ts),
    }

# ─── FRAUD PATTERN INJECTION ─────────────────────────────────────────────────

def inject_velocity_cluster(base_ts: datetime, customer: dict,
                            merchant: dict, channel: str, cfg: dict,
                            txn_counter: list) -> list[dict]:
    # IEEE-CIS pattern: velocity fraud clusters thường amount nhỏ, nhiều lần nhanh
    records = []
    n_txn = random.randint(3, 5)
    for i in range(n_txn):
        ts = base_ts + timedelta(seconds=random.randint(0, 55))
        txn_counter[0] += 1
        txn_id = f"TXN{txn_counter[0]:010d}"
        amount = random.randint(50_000, 500_000)  # nhỏ — pattern thực tế
        rec = generate_normal_txn(txn_id, ts, customer, merchant, channel, cfg)
        rec.update({"amount": amount, "fraud_label": 1, "fraud_type": "velocity"})
        records.append(rec)
    return records

def inject_geo_anomaly(base_ts: datetime, customer: dict,
                       channel: str, cfg: dict,
                       txn_counter: list, merchants: list) -> list[dict]:
    records = []
    # Txn 1: tỉnh gốc
    mer1 = next((m for m in merchants
                 if m["province"] == customer["home_province"]), merchants[0])
    txn_counter[0] += 1
    rec1 = generate_normal_txn(f"TXN{txn_counter[0]:010d}",
                               base_ts, customer, mer1, channel, cfg)
    rec1.update({"fraud_label": 1, "fraud_type": "geo_anomaly"})
    records.append(rec1)

    # Txn 2: tỉnh xa (khác tỉnh gốc, cách > 300km)
    far_locations = [l for l in VN_LOCATIONS
                     if l["province"] != customer["home_province"]]
    far_loc = random.choice(far_locations)
    far_merchant = next((m for m in merchants
                         if m["province"] == far_loc["province"]), merchants[-1])
    ts2 = base_ts + timedelta(minutes=random.randint(5, 25))
    txn_counter[0] += 1
    rec2 = generate_normal_txn(f"TXN{txn_counter[0]:010d}",
                               ts2, customer, far_merchant, channel, cfg)
    rec2.update({"fraud_label": 1, "fraud_type": "geo_anomaly"})
    records.append(rec2)
    return records

def inject_off_hours_large(base_ts: datetime, customer: dict,
                           merchant: dict, channel: str, cfg: dict,
                           txn_counter: list) -> list[dict]:
    # Force giờ khuya nếu base_ts không phải khuya
    ts = base_ts.replace(hour=random.randint(22, 23)
                         if base_ts.hour < 22 else base_ts.hour)
    txn_counter[0] += 1
    amount = random.randint(50_000_001, 200_000_000)
    rec = generate_normal_txn(f"TXN{txn_counter[0]:010d}",
                              ts, customer, merchant, channel, cfg)
    rec.update({"amount": amount,
                "fraud_label": 1, "fraud_type": "off_hours_large"})
    return [rec]

def inject_duplicate(base_ts: datetime, customer: dict,
                     merchant: dict, channel: str, cfg: dict,
                     txn_counter: list) -> list[dict]:
    amount = random.randint(100_000, 5_000_000)
    records = []
    for i in range(2):
        ts = base_ts + timedelta(seconds=random.randint(0, 28))
        txn_counter[0] += 1
        rec = generate_normal_txn(f"TXN{txn_counter[0]:010d}",
                                  ts, customer, merchant, channel, cfg)
        rec.update({"amount": amount,
                    "fraud_label": 1, "fraud_type": "duplicate"})
        records.append(rec)
    return records

# ─── MAIN GENERATOR ──────────────────────────────────────────────────────────

def generate_dataset(config: dict = CONFIG) -> dict[str, pd.DataFrame]:
    random.seed(config["seed"])
    np.random.seed(config["seed"])
    fake = Faker("vi_VN")
    Faker.seed(config["seed"])

    os.makedirs(config["output_dir"], exist_ok=True)

    print("Generating customers and merchants...")
    customers = generate_customers(config["num_customers"], fake)
    merchants = generate_merchants(config["num_merchants"], fake)

    n_total   = config["total_transactions"]
    n_fraud   = int(n_total * config["fraud_rate"])
    n_normal  = n_total - n_fraud

    base_date = date.today()
    timestamps = generate_timestamps(n_total, base_date)

    channels = list(CHANNEL_CONFIG.keys())
    ch_weights = [CHANNEL_CONFIG[c]["weight"] for c in channels]

    all_records = []
    txn_counter = [0]  # mutable để pass vào hàm

    # ── Sinh normal transactions ──
    print(f"Generating {n_normal:,} normal transactions...")
    for i in tqdm(range(n_normal)):
        ts       = timestamps[i]
        channel  = random.choices(channels, weights=ch_weights)[0]
        cfg      = CHANNEL_CONFIG[channel]
        customer = random.choice(customers)
        merchant = random.choice(merchants)
        txn_counter[0] += 1
        txn_id = f"TXN{txn_counter[0]:010d}"
        rec = generate_normal_txn(txn_id, ts, customer, merchant, channel, cfg)
        all_records.append(rec)

    # ── Inject fraud patterns ──
    print(f"Injecting {n_fraud:,} fraud records ({config['fraud_rate']*100:.0f}%)...")
    fraud_types = ["velocity", "geo_anomaly", "off_hours_large", "duplicate"]
    fraud_weights = [0.30, 0.30, 0.25, 0.15]

    injected = 0
    with tqdm(total=n_fraud) as pbar:
        while injected < n_fraud:
            fraud_type = random.choices(fraud_types, weights=fraud_weights)[0]
            ts       = random.choice(timestamps)
            channel  = random.choices(channels, weights=ch_weights)[0]
            cfg      = CHANNEL_CONFIG[channel]
            customer = random.choice(customers)
            merchant = random.choice(merchants)

            if fraud_type == "velocity":
                recs = inject_velocity_cluster(ts, customer, merchant, channel, cfg, txn_counter)
            elif fraud_type == "geo_anomaly":
                recs = inject_geo_anomaly(ts, customer, channel, cfg, txn_counter, merchants)
            elif fraud_type == "off_hours_large":
                recs = inject_off_hours_large(ts, customer, merchant, channel, cfg, txn_counter)
            else:
                recs = inject_duplicate(ts, customer, merchant, channel, cfg, txn_counter)

            all_records.extend(recs)
            injected += len(recs)
            pbar.update(len(recs))

    # ── Sort by timestamp ──
    df = pd.DataFrame(all_records)
    df = df.sort_values("timestamp").reset_index(drop=True)

    # ── Split theo kênh và format ──
    print("Splitting by channel and saving...")
    results = {}

    # ATM + POS → CSV (giả lập file log từ máy ATM/POS)
    for ch in ["ATM", "POS"]:
        ch_df = df[df["channel"] == ch].copy()
        path = f"{config['output_dir']}/txn_{ch.lower()}.csv"
        ch_df.to_csv(path, index=False)
        results[ch] = ch_df
        print(f"  {ch}: {len(ch_df):,} records → {path}")

    # Mobile + Internet → JSON (giả lập REST API / webhook payload)
    for ch in ["mobile", "internet"]:
        ch_df = df[df["channel"] == ch].copy()
        records_json = ch_df.to_dict(orient="records")
        path = f"{config['output_dir']}/txn_{ch}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records_json, f, ensure_ascii=False, indent=2,
                      default=str)
        results[ch] = ch_df
        print(f"  {ch}: {len(ch_df):,} records → {path}")

    # All channels → 1 CSV tổng (để phân tích)
    all_path = f"{config['output_dir']}/txn_all.csv"
    df.to_csv(all_path, index=False)

    # Customers dim table
    cus_df = pd.DataFrame(customers)
    cus_path = f"{config['output_dir']}/dim_customer.csv"
    cus_df.to_csv(cus_path, index=False)

    # Merchants dim table
    mer_df = pd.DataFrame(merchants)
    mer_path = f"{config['output_dir']}/dim_merchant.csv"
    mer_df.to_csv(mer_path, index=False)

    results["all"] = df
    results["customers"] = cus_df
    results["merchants"] = mer_df

    # ── Summary report ──
    print("\n" + "="*55)
    print("DATASET SUMMARY")
    print("="*55)
    print(f"Total transactions : {len(df):,}")
    print(f"Fraud transactions : {df['fraud_label'].sum():,} "
          f"({df['fraud_label'].mean()*100:.2f}%)")
    print(f"Date range         : {df['timestamp'].min()} → {df['timestamp'].max()}")
    print(f"\nBy channel:")
    for ch, cnt in df['channel'].value_counts().items():
        print(f"  {ch:12s}: {cnt:,} ({cnt/len(df)*100:.1f}%)")
    print(f"\nBy fraud type:")
    fraud_df = df[df['fraud_label']==1]
    for ft, cnt in fraud_df['fraud_type'].value_counts().items():
        print(f"  {ft:20s}: {cnt:,}")
    print(f"\nAmount stats (VND):")
    print(f"  Normal  — mean: {df[df['fraud_label']==0]['amount'].mean():,.0f}")
    print(f"  Fraud   — mean: {df[df['fraud_label']==1]['amount'].mean():,.0f}")
    print(f"\nPeak hour txns     : {df['is_peak_hour'].sum():,} "
          f"({df['is_peak_hour'].mean()*100:.1f}%)")
    print(f"Business hour txns : {df['is_business_hour'].sum():,} "
          f"({df['is_business_hour'].mean()*100:.1f}%)")
    print(f"\nFiles saved to: ./{config['output_dir']}/")
    print("="*55)

    return results


# ─── NOTEBOOK HELPER ─────────────────────────────────────────────────────────

def quick_analysis(df: pd.DataFrame):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Banking Transaction Dataset — Quick Analysis", fontsize=14)

    # 1. Txn theo giờ
    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    hourly = df.groupby(['hour', 'fraud_label']).size().unstack(fill_value=0)
    hourly.plot(ax=axes[0,0], kind='bar', stacked=False,
                color=['steelblue','crimson'], alpha=0.8)
    axes[0,0].set_title("Transactions by Hour")
    axes[0,0].set_xlabel("Hour of Day")
    axes[0,0].legend(["Normal", "Fraud"])

    # 2. Amount distribution (log scale)
    axes[0,1].hist(df[df['fraud_label']==0]['amount'],
                   bins=50, alpha=0.6, color='steelblue', label='Normal')
    axes[0,1].hist(df[df['fraud_label']==1]['amount'],
                   bins=50, alpha=0.6, color='crimson', label='Fraud')
    axes[0,1].set_xscale('log')
    axes[0,1].set_title("Amount Distribution (log scale)")
    axes[0,1].legend()

    # 3. Channel breakdown
    ch_counts = df['channel'].value_counts()
    axes[1,0].pie(ch_counts.values, labels=ch_counts.index,
                  autopct='%1.1f%%', startangle=90)
    axes[1,0].set_title("Transactions by Channel")

    # 4. Fraud type breakdown
    fraud_df = df[df['fraud_label']==1]
    ft_counts = fraud_df['fraud_type'].value_counts()
    axes[1,1].barh(ft_counts.index, ft_counts.values, color='crimson', alpha=0.7)
    axes[1,1].set_title("Fraud Patterns Injected")

    plt.tight_layout()
    plt.savefig("output/dataset_analysis.png", dpi=150, bbox_inches='tight')
    print("Chart saved to output/dataset_analysis.png")
    plt.show()


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    results = generate_dataset(CONFIG)
    quick_analysis(results["all"])
