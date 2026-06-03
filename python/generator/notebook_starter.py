from data_generator import generate_dataset, quick_analysis, CONFIG

MY_CONFIG = {
    **CONFIG,
    "total_transactions": 50_000,
    "fraud_rate": 0.02,
    "simulation_days": 1,
    "output_dir": "output",
    "seed": 42,
}

results = generate_dataset(MY_CONFIG)

df = results["all"]
print(df.head(10))
print(f"\nShape: {df.shape}")
print(f"Fraud rate: {df['fraud_label'].mean()*100:.2f}%")

quick_analysis(df)

import pandas as pd

fraud_df = df[df['fraud_label'] == 1]
print("\n=== FRAUD PATTERNS ===")
print(fraud_df['fraud_type'].value_counts())

print("\n=== VELOCITY SAMPLE (3 txn cùng account trong 60s) ===")
vel = fraud_df[fraud_df['fraud_type'] == 'velocity'].sort_values('timestamp')
if len(vel) > 0:
    sample_account = vel.iloc[0]['account_masked']
    print(vel[vel['account_masked'] == sample_account][
        ['timestamp', 'account_masked', 'amount', 'merchant_province']
    ].head(5))

print("\n=== GEO ANOMALY SAMPLE (2 tỉnh trong 30 phút) ===")
geo = fraud_df[fraud_df['fraud_type'] == 'geo_anomaly'].sort_values('timestamp')
if len(geo) > 1:
    sample_account = geo.iloc[0]['account_masked']
    print(geo[geo['account_masked'] == sample_account][
        ['timestamp', 'account_masked', 'merchant_province', 'amount']
    ].head(4))

print("\n=== OFF-HOURS LARGE SAMPLE (>50M VND lúc 22h-6h) ===")
off = fraud_df[fraud_df['fraud_type'] == 'off_hours_large']
print(off[['timestamp', 'account_masked', 'amount', 'merchant_province']].head(5))

print("\n=== DATA QUALITY CHECKS ===")
pii_leak = df[~df['account_number'].str.startswith('VCB')]
print(f"[OK] All accounts have VCB prefix: {len(pii_leak) == 0}")
mask_ok = df['account_masked'].str.startswith('****')
print(f"[OK] All masked accounts start with ****: {mask_ok.all()}")
print(f"[OK] No null transaction_id: {df['transaction_id'].notna().all()}")
print(f"[OK] All amounts > 0: {(df['amount'] > 0).all()}")
fraud_rate = df['fraud_label'].mean()
print(f"[OK] Fraud rate {fraud_rate*100:.2f}% (target: ~2%): "
      f"{0.01 <= fraud_rate <= 0.05}")

print("\n=== FILES GENERATED ===")
import os
for f in sorted(os.listdir('output')):
    size = os.path.getsize(f'output/{f}')
    print(f"  {f:30s} {size/1024:.1f} KB")
