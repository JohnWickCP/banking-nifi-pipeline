"""Generate banking pipeline workflow diagram — docs/pipeline_workflow.png"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from matplotlib.lines import Line2D

# ── PALETTE ──────────────────────────────────────────────────────────────────
BG          = '#0F172A'
CARD_BG     = '#1E293B'
C_GREEN_BG  = '#052e16';  C_GREEN_BORDER  = '#10B981';  C_GREEN_TEXT  = '#34d399'
C_PURPLE_BG = '#1e1b4b';  C_PURPLE_BORDER = '#7c3aed';  C_PURPLE_TEXT = '#c4b5fd'
C_ORANGE_BG = '#431407';  C_ORANGE_BORDER = '#f97316';  C_ORANGE_TEXT = '#fed7aa'
C_RED_BG    = '#2d0808';  C_RED_BORDER    = '#ef4444';  C_RED_TEXT    = '#fca5a5'
C_BLUE_BG   = '#0c1a3a';  C_BLUE_BORDER   = '#3b82f6';  C_BLUE_TEXT   = '#93c5fd'
C_GRAY_BG   = '#111827';  C_GRAY_BORDER   = '#475569';  C_GRAY_TEXT   = '#94a3b8'
WHITE       = '#f8fafc';  MUTED           = '#64748b'

fig = plt.figure(figsize=(22, 12), facecolor=BG)
ax  = fig.add_axes([0.01, 0.01, 0.98, 0.98])
ax.set_xlim(0, 22); ax.set_ylim(0, 12)
ax.axis('off')

def box(x, y, w, h, fc, ec, lw=1.2, radius=0.12, alpha=1.0):
    p = FancyBboxPatch((x, y), w, h,
                       boxstyle=f'round,pad=0.05,rounding_size={radius}',
                       facecolor=fc, edgecolor=ec, linewidth=lw, alpha=alpha,
                       zorder=2)
    ax.add_patch(p)

def arrow(x1, y1, x2, y2, color, lw=1.4, style='->'):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color,
                                lw=lw, connectionstyle='arc3,rad=0'),
                zorder=3)

def txt(x, y, s, color=WHITE, size=9, weight='normal', ha='center', va='center'):
    ax.text(x, y, s, ha=ha, va=va, fontsize=size, fontweight=weight,
            color=color, zorder=4)

# ════════════════════════════════════════════════════════════════════════════
# TITLE
# ════════════════════════════════════════════════════════════════════════════
txt(11, 11.6, 'Banking Multi-channel Pipeline — Real-time Transaction Journey',
    color=WHITE, size=16, weight='bold')
txt(11, 11.25, 'Apache NiFi  ·  Kafka 3.6  ·  PostgreSQL 15  ·  MinIO  ·  Grafana 10  ·  Docker Compose',
    color=MUTED, size=10)

# ════════════════════════════════════════════════════════════════════════════
# ROW 1 — DATA SOURCES  (y = 9.3 → 10.7)
# ════════════════════════════════════════════════════════════════════════════
src_y = 9.3; src_h = 1.4

sources = [
    ('ATM\nCSV logs', 0.3, C_GREEN_BG, C_GREEN_BORDER,  'Avg: 2,000,000 VND', '****6789'),
    ('POS\nCSV logs', 3.1, C_GREEN_BG, C_GREEN_BORDER,  'Avg:   500,000 VND', '****1234'),
    ('Mobile\nREST API', 5.9, C_GREEN_BG, C_GREEN_BORDER,'Avg: 1,200,000 VND', '****4321'),
    ('Internet BK\nHTTPS',  8.7, C_GREEN_BG, C_GREEN_BORDER,'Avg: 5,000,000 VND', '****5678'),
]
for label, sx, fc, ec, amount, acct in sources:
    box(sx, src_y, 2.5, src_h, fc, ec)
    ch, sub = label.split('\n')
    txt(sx+1.25, src_y+1.1, ch,   color='#86efac', size=10, weight='bold')
    txt(sx+1.25, src_y+0.78, sub, color=C_GREEN_TEXT, size=8)
    txt(sx+1.25, src_y+0.48, amount, color=MUTED, size=7.5)
    txt(sx+1.25, src_y+0.22, f'account: {acct}', color=MUTED, size=7)

# Kafka box
box(12.0, src_y, 3.2, src_h, C_PURPLE_BG, C_PURPLE_BORDER, lw=1.4)
txt(13.6, src_y+1.1,  'Apache Kafka 3.6', color='#e0e7ff', size=11, weight='bold')
txt(13.6, src_y+0.78, 'Message Broker',   color=C_PURPLE_TEXT, size=9)
txt(13.6, src_y+0.48, 'txn.raw  ·  txn.alert', color='#818cf8', size=8)
txt(13.6, src_y+0.22, 'txn.dead-letter', color='#818cf8', size=8)

# Arrows sources → Kafka
for sx in [1.55, 4.35, 7.15, 9.95]:
    arrow(sx, src_y+0.7, 12.0, src_y+0.7, C_GREEN_BORDER, lw=1.3)

# ════════════════════════════════════════════════════════════════════════════
# ROW 2 — NIFI ENGINE  (y = 5.8 → 9.0)
# ════════════════════════════════════════════════════════════════════════════

# Down arrow Kafka → NiFi
arrow(13.6, src_y, 13.6, 8.88, C_PURPLE_BORDER, lw=1.5)

# Outer NiFi container
box(0.2, 5.8, 21.6, 3.1, '#060316', C_PURPLE_BORDER, lw=1, alpha=0.6)
txt(0.55, 8.67, 'Apache NiFi 1.23 — Business-aware Processing', color='#a78bfa', size=9, ha='left')

nifi_y = 6.1; nifi_h = 2.5; gap = 0.15

# ── ① Business Context Engine ──────────────────────────────────────────────
box(0.35, nifi_y, 4.0, nifi_h, C_ORANGE_BG, C_ORANGE_BORDER)
txt(2.35, nifi_y+2.2, '① Business Context', color='#fcd34d', size=9.5, weight='bold')
bc_items = [
    ('[PEAK] hours: 8–11h, 13–16h', C_ORANGE_TEXT),
    ('[NIGHT] 22h–06h (alert if large txn)', '#fdba74'),
    ('[HOLIDAY] VN holidays 2025 (11 dates)', '#fdba74'),
    ('[MAINT] Sat 01h–03h (auto-pause)', '#fdba74'),
    ('>> tags: is_peak · is_night · is_holiday', '#d97706'),
]
for i, (s, c) in enumerate(bc_items):
    txt(2.35, nifi_y+1.82-i*0.33, s, color=c, size=7.5)

# ── ② Validate + Route ────────────────────────────────────────────────────
box(4.65, nifi_y, 3.6, nifi_h, C_BLUE_BG, C_BLUE_BORDER)
txt(6.45, nifi_y+2.2, '② Validate + Route', color=C_BLUE_TEXT, size=9.5, weight='bold')
val_items = [
    ('ValidateRecord (JSON schema)', C_BLUE_TEXT),
    ('[OK]  valid (99.99%)  →  next',   '#34d399'),
    ('[ERR] missing required fields',   '#fca5a5'),
    ('   → txn.dead-letter topic',  '#ef4444'),
    ('Error rate:  < 0.01%',         MUTED),
]
for i, (s, c) in enumerate(val_items):
    txt(6.45, nifi_y+1.82-i*0.33, s, color=c, size=7.5)

# ── ③ Jolt + Enrich ───────────────────────────────────────────────────────
box(8.45, nifi_y, 3.8, nifi_h, C_BLUE_BG, C_BLUE_BORDER)
txt(10.35, nifi_y+2.2, '③ Jolt + Enrich', color=C_BLUE_TEXT, size=9.5, weight='bold')
j_items = [
    ('Before: {amt, acctNo, ...}',        MUTED),
    ('JoltTransform: 4ch → 1 schema',     C_BLUE_TEXT),
    ('LookupRecord: dim_customer join',    C_BLUE_TEXT),
    ('After: {amount, account_masked,',    '#c4b5fd'),
    ('  customer_segment: "VIP"}',         '#c4b5fd'),
]
for i, (s, c) in enumerate(j_items):
    txt(10.35, nifi_y+1.82-i*0.33, s, color=c, size=7.5)

# ── ④ Fraud Detection Engine ──────────────────────────────────────────────
box(12.45, nifi_y, 5.2, nifi_h, C_RED_BG, C_RED_BORDER, lw=1.4)
txt(15.05, nifi_y+2.2, '④ Fraud Detection Engine', color='#fca5a5', size=9.5, weight='bold')
fd_items = [
    ('R1  Velocity: ≥3 txn same acct / 60s',    '→ MEDIUM', '#fbbf24'),
    ('R2  Geo-anomaly: >300km in <30 min',       '→ HIGH',   '#f87171'),
    ('R3  Off-hours: >50M VND (22h–06h)',        '→ HIGH ★', '#f87171'),
    ('R4  Duplicate: same acct+amt+merch / 30s', '→ LOW',    '#fb923c'),
]
for i, (rule, sev, c) in enumerate(fd_items):
    txt(12.7, nifi_y+1.82-i*0.38, rule, color='#e2e8f0', size=7.5, ha='left')
    txt(17.45, nifi_y+1.82-i*0.38, sev, color=c, size=7.5, weight='bold', ha='right')
txt(15.05, nifi_y+0.22, 'All rules share 1 DistributedMapCacheClient — no extra services',
    color=MUTED, size=7, ha='center')

# ── ⑤ PII Mask + Store ────────────────────────────────────────────────────
box(17.85, nifi_y, 3.75, nifi_h, '#0a1628', '#60a5fa')
txt(19.72, nifi_y+2.2, '⑤ PII Mask + Store', color='#93c5fd', size=9.5, weight='bold')
pii_items = [
    ('VCB0123456789 → ****6789',      '#94a3b8'),
    ('0901234567  → 090****567',      '#94a3b8'),
    ('Consistent hash (join-safe)',    '#60a5fa'),
    ('PutDatabaseRecord → fact_txn',  '#34d399'),
    ('PutS3Object → MinIO raw JSON',  '#34d399'),
]
for i, (s, c) in enumerate(pii_items):
    txt(19.72, nifi_y+1.82-i*0.33, s, color=c, size=7.5)

# Internal arrows  ①→②→③→④→⑤
for x1, x2, mid_y in [
    (4.35, 4.65, nifi_y+1.25),
    (8.25, 8.45, nifi_y+1.25),
    (12.25, 12.45, nifi_y+1.25),
    (17.65, 17.85, nifi_y+1.25),
]:
    arrow(x1, mid_y, x2, mid_y, C_PURPLE_TEXT, lw=1.3)

# ════════════════════════════════════════════════════════════════════════════
# ROW 3 — OUTPUT PATHS  (y = 3.4 → 5.5)
# ════════════════════════════════════════════════════════════════════════════
out_y = 3.5

# Down arrows NiFi → outputs
for ox, oc in [(5.5, C_GREEN_BORDER), (11.2, C_RED_BORDER), (17.5, C_GRAY_BORDER)]:
    arrow(ox, nifi_y, ox, out_y+1.5, oc, lw=1.5)

# ── Clean path ────────────────────────────────────────────────────────────
box(1.8, out_y, 7.0, 1.6, C_GREEN_BG, C_GREEN_BORDER, lw=1.4)
txt(5.3, out_y+1.3, '[OK]  CLEAN PATH', color='#34d399', size=11, weight='bold')
txt(5.3, out_y+0.9, 'PostgreSQL fact_txn  (9,999 rows)  +  MinIO banking-raw bucket', color='#6ee7b7', size=8.5)
txt(5.3, out_y+0.55, 'Star schema: fact_txn → dim_customer · dim_time · dim_calendar (VN holidays)', color='#4ade80', size=7.5)
txt(5.3, out_y+0.22, 'ON CONFLICT DO NOTHING — idempotent upsert', color=MUTED, size=7)

# ── Alert path ────────────────────────────────────────────────────────────
box(9.2, out_y, 5.8, 1.6, C_RED_BG, C_RED_BORDER, lw=1.4)
txt(12.1, out_y+1.3, '[!!]  FRAUD ALERT', color='#fca5a5', size=11, weight='bold')
txt(12.1, out_y+0.9, 'Kafka txn.alert topic + PostgreSQL fact_alert', color='#fca5a5', size=8.5)
txt(12.1, out_y+0.55, 'Fields: rule_triggered · severity · detected_at', color='#f87171', size=7.5)
txt(12.1, out_y+0.22, 'Detected in ~3s p50  /  ~5s p95  ← target <5s ✓', color='#fb923c', size=7.5, weight='bold')

# ── Dead-letter path ─────────────────────────────────────────────────────
box(15.2, out_y, 5.5, 1.6, C_GRAY_BG, C_GRAY_BORDER)
txt(17.95, out_y+1.3, '[X]  DEAD-LETTER', color='#94a3b8', size=11, weight='bold')
txt(17.95, out_y+0.9, 'txn.dead-letter topic (7-day retention)', color='#94a3b8', size=8.5)
txt(17.95, out_y+0.55, 'Schema violations · missing required fields', color=MUTED, size=7.5)
txt(17.95, out_y+0.22, 'Replay: python scripts/replay_dead_letter.py', color=MUTED, size=7)

# ════════════════════════════════════════════════════════════════════════════
# ROW 4 — GRAFANA  +  BENCHMARK  (y = 0.5 → 3.1)
# ════════════════════════════════════════════════════════════════════════════
viz_y = 0.5

# Down arrows
for ox, oc in [(5.5, '#22c55e'), (12.1, '#ef4444')]:
    arrow(ox, out_y, ox, viz_y+2.4, oc, lw=1.2)

# ── Grafana ───────────────────────────────────────────────────────────────
box(0.4, viz_y, 8.0, 2.6, '#061a06', '#22c55e', lw=1.2)
txt(4.4, viz_y+2.35, 'Grafana — Live Dashboard', color='#86efac', size=10.5, weight='bold')
grafana_panels = [
    '• Transactions / hour  (heatmap by channel)',
    '• Fraud alerts by rule: velocity · geo · off-hours · dup',
    '• P50 / P95 fraud detection latency',
    '• Kafka consumer lag  ·  dead-letter queue depth',
    '• RAM usage per service  (docker stats)',
]
for i, p in enumerate(grafana_panels):
    txt(4.4, viz_y+1.88-i*0.38, p, color='#4ade80', size=8)

# ── Benchmark callout ────────────────────────────────────────────────────
box(9.0, viz_y, 12.5, 2.6, '#0c0c1e', C_PURPLE_BORDER, lw=1.2)
txt(15.25, viz_y+2.35, 'Benchmark Results  (measured — 10,000 rows)',
    color=C_PURPLE_TEXT, size=10.5, weight='bold')

metrics = [
    ('NiFi throughput (full ETL)',       '997 rows/s',         '(validate + transform + 4 fraud rules + dual store)'),
    ('Baseline Python ETL',              '2,405 rows/s',       '(raw psycopg2 insert, no enrichment)'),
    ('Fraud detection latency',          'p50 = ~3s  /  p95 = ~5s', '← target < 5 sec  ✓'),
    ('PII masking coverage',             '100%',               '(0 unmasked accounts in 10k+ records)  ✓'),
    ('Docker RAM usage',                 '3.87 GiB total',     '← target < 6 GB  ✓'),
    ('Dead-letter error rate',           '< 0.01%',            '(schema-invalid / total processed)  ✓'),
]
col_x = [9.3, 13.0, 15.9]
for i, (label, val, note) in enumerate(metrics):
    row = i % 3; col = i // 3
    base_y = viz_y + 1.85 - row * 0.6
    offset = col * 10.5
    x0 = col_x[col]
    txt(x0,      base_y,       label + ':', color='#94a3b8', size=7.5, ha='left')
    txt(x0+0.15, base_y-0.26,  val,         color='#c4b5fd', size=8.5, weight='bold', ha='left')
    txt(x0+0.15, base_y-0.50,  note,        color=MUTED,     size=7,   ha='left')

# ── docker compose up callout ─────────────────────────────────────────────
box(9.0, 0.02, 12.5, 0.42, '#0a0a1a', '#7c3aed', lw=1, radius=0.06)
txt(15.25, 0.23, '$ docker compose up -d     →  full stack ready in < 3 minutes  ·  reviewer can clone & run immediately',
    color='#a78bfa', size=8.5, ha='center')

# ════════════════════════════════════════════════════════════════════════════
# LEGEND (bottom-left)
# ════════════════════════════════════════════════════════════════════════════
legend_items = [
    (C_GREEN_BORDER, 'Data sources'),
    (C_ORANGE_BORDER, 'Business context'),
    (C_BLUE_BORDER, 'NiFi processing'),
    (C_RED_BORDER, 'Fraud detection / alert'),
    ('#22c55e', 'Visualization'),
    (C_PURPLE_BORDER, 'Measured benchmark'),
]
for i, (c, lbl) in enumerate(legend_items):
    lx = 0.4 + i * 3.6
    ax.plot([lx, lx+0.4], [0.22, 0.22], color=c, lw=2.5, solid_capstyle='round', zorder=5)
    txt(lx+0.55, 0.22, lbl, color=MUTED, size=7.5, ha='left')

plt.savefig('docs/pipeline_workflow.png', dpi=140,
            bbox_inches='tight', facecolor=BG, edgecolor='none')
print('Saved: docs/pipeline_workflow.png')
