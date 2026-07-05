import os, json, warnings
warnings.filterwarnings("ignore")

# deteksi environment: container atau lokal
IS_CONTAINER = (
    os.path.exists("/.dockerenv")
    or os.path.exists("/opt/flink/bin/flink")
)

if not IS_CONTAINER:
    try:
        from dotenv import load_dotenv
        _SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
        _PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
        _ENV_PATH     = os.path.join(_PROJECT_ROOT, "docker", ".env")
        if os.path.exists(_ENV_PATH):
            load_dotenv(dotenv_path=_ENV_PATH, override=True)
            print(f"[.env] Dimuat dari: {os.path.normpath(_ENV_PATH)}", flush=True)
        else:
            print(f"[WARN] .env tidak ditemukan: {os.path.normpath(_ENV_PATH)}", flush=True)
    except ImportError:
        print("[WARN] python-dotenv tidak terinstall, env vars dibaca dari sistem.", flush=True)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import psycopg2
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# path dan konfigurasi database
if IS_CONTAINER:
    MODEL_DIR   = "/opt/data/models"
    OUTPUT_DIR  = "/opt/data/processed"
    PG_CONFIG   = {
        "host":     os.environ.get("POSTGRES_HOST",     "postgres-hasil"),
        "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
        "database": os.environ.get("POSTGRES_DATABASE", "hasil_klasifikasi"),
        "user":     os.environ.get("POSTGRES_USER",     "skripsi"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }
else:
    ROOT        = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    MODEL_DIR   = os.path.join(ROOT, "notebooks", "data", "models")
    OUTPUT_DIR  = os.path.join(ROOT, "notebooks", "data", "processed")
    PG_CONFIG   = {
        "host":     os.environ.get("POSTGRES_HOST_LOCAL", "127.0.0.1"),
        "port":     int(os.environ.get("POSTGRES_PORT",   5432)),
        "database": os.environ.get("POSTGRES_DATABASE",   "hasil_klasifikasi"),
        "user":     os.environ.get("POSTGRES_USER",       "skripsi"),
        "password": os.environ.get("POSTGRES_PASSWORD",   ""),
    }

os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Mode     : {'CONTAINER' if IS_CONTAINER else 'LOKAL'}")
print(f"Output   : {OUTPUT_DIR}")
print(f"PG host  : {PG_CONFIG['host']}:{PG_CONFIG['port']}")

def out(filename):
    return os.path.join(OUTPUT_DIR, filename)
def kat_metrik(v):
    if v >= 0.90: return "Sangat Baik"
    if v >= 0.80: return "Baik"
    if v >= 0.70: return "Cukup"
    return "Perlu Perbaikan"
def kategori_latency(ms):
    if ms < 1000: return "Sangat Baik (< 1 detik)"
    if ms < 2000: return "Baik (1–2 detik)"
    if ms < 3000: return "Cukup (2–3 detik)"
    return "Buruk (> 3 detik)"

# ============================================================
# AMBIL DATA HASIL KLASIFIKASI DARI POSTGRESQL
# ============================================================
print("\n" + "=" * 65)
print("Langkah 1: Mengambil Data Hasil Klasifikasi dari PostgreSQL")
print("=" * 65)

conn = psycopg2.connect(**PG_CONFIG)
df   = pd.read_sql("SELECT * FROM classification_result ORDER BY id", conn)
conn.close()

print(f"  total event hasil streaming  : {len(df):,}")
print(f"  kolom yang tersedia          : {list(df.columns)}")
print(f"\n  sampel data (3 baris pertama):")
print(df.head(3).to_string())

WINDOW_SIZE = 1 

# ============================================================
# DISTRIBUSI LABEL HASIL KLASIFIKASI STREAMING
# ============================================================
print("\n" + "=" * 65)
print("Langkah 2: Distribusi label hasil klasifikasi streaming")
print("=" * 65)

# Kumpulkan semua baris terlebih dahulu untuk menghitung lebar kolom
distribusi_data = []
for model_col, nama in [("label_knn", "kNN"), ("label_lr", "Logistic Regression")]:
    vc = df[model_col].value_counts()
    for lbl, cnt in vc.items():
        distribusi_data.append((nama, lbl, cnt, cnt / len(df)))

w_model = max((len(d[0]) for d in distribusi_data), default=5)
w_model = max(w_model, len("Model"))
w_label = max((len(d[1]) for d in distribusi_data), default=5)
w_label = max(w_label, len("Label"))
w_jml   = 10
w_pct   = 10

print()
print(f"  {'Model':<{w_model}}  {'Label':<{w_label}}  {'Jumlah':>{w_jml}}  {'Persentase':>{w_pct}}")
print(f"  {'─'*w_model}  {'─'*w_label}  {'─'*w_jml}  {'─'*w_pct}")

for nama, lbl, cnt, pct in distribusi_data:
    print(f"  {nama:<{w_model}}  {lbl:<{w_label}}  {cnt:>{w_jml},}  {pct:>{w_pct}.1%}")

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
for ax, col, nama, color in [
    (axes[0], "label_knn", "kNN",                 ["steelblue", "tomato"]),
    (axes[1], "label_lr",  "Logistic Regression", ["seagreen",  "tomato"]),
]:
    vc = df[col].value_counts()
    vc.plot(kind="bar", ax=ax, color=color, edgecolor="white")
    ax.set_title(f"Distribusi Label — {nama}")
    ax.set_xlabel("Label"); ax.set_ylabel("Jumlah")
    ax.tick_params(axis="x", rotation=0)
    for bar in ax.patches:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f"{int(bar.get_height()):,}", ha="center", fontsize=9, fontweight="bold")
plt.suptitle("Distribusi Hasil Klasifikasi — Streaming Analytics", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig(out("distribusi_label_streaming.png"), dpi=150, bbox_inches="tight")
plt.close()

# ============================================================
# EVALUASI METRIK AWAL (offline — dari metrics_awal.json)
# ============================================================
print("\n" + "=" * 65)
print("Langkah 3: Evaluasi Metrik Awal")
print("=" * 65)
print("  sumber: metrics_awal.json (hasil evaluasi model sebelum streaming)")

metrics_path = os.path.join(MODEL_DIR, "metrics_awal.json")
metrics_awal = None
try:
    with open(metrics_path, encoding="utf-8") as f:
        metrics_awal = json.load(f)
    print(f"\n  {'Metrik':<14} {'kNN':>12} {'LR':>12}")
    print(f"  {'─'*14} {'─'*12} {'─'*12}")
    for m in ["accuracy", "precision", "recall", "f1_score"]:
        kv = metrics_awal["knn"][m]
        lv = metrics_awal["logistic_regression"][m]
        print(f"  {m:<14} {kv:>12.4f} {lv:>12.4f}")
except FileNotFoundError:
    print(f"  metrics_awal.json tidak ditemukan di: {metrics_path}")
    print("  jalankan dulu 03_model_training.py.")

# ============================================================
# EVALUASI METRIK AKHIR (dari data hasil streaming PostgreSQL)
# ============================================================
print("\n" + "=" * 65)
print("Langkah 4: Evaluasi Metrik Akhir")
print("=" * 65)
print(f"  total event yang dievaluasi  : {len(df):,}")
print(f"  metode: prediksi kNN & LR dibandingkan terhadap ground_truth")
print(f"          (ground_truth diturunkan dari eventid — konsisten dengan preprocessing)")
print()

# Validasi kolom ground_truth tersedia
if "ground_truth" not in df.columns:
    raise ValueError(
        "Kolom 'ground_truth' tidak ditemukan di tabel classification_result.\n"
        "Pastikan init.sql sudah menambahkan kolom ground_truth dan "
        "streaming_job_container.py mengisinya saat insert berdasarkan ANOMALI_EVENTS."
    )

n_missing = df["ground_truth"].isna().sum()
if n_missing > 0:
    print(f"  [WARN] {n_missing:,} baris memiliki ground_truth NULL — baris ini dikecualikan dari evaluasi.")
    df = df.dropna(subset=["ground_truth"])
    print(f"  total event setelah filter  : {len(df):,}")
    print()

# ground_truth: 1 = anomali, 0 = normal (sesuai logika preprocessing)
y_true    = df["ground_truth"].astype(int).values
knn_pred  = (df["label_knn"] == "anomali").astype(int).values
lr_pred   = (df["label_lr"]  == "anomali").astype(int).values

# Evaluasi kNN terhadap ground_truth
acc_knn   = accuracy_score(y_true,  knn_pred)
prec_knn  = precision_score(y_true, knn_pred, zero_division=0)
rec_knn   = recall_score(y_true,   knn_pred, zero_division=0)
f1_knn    = f1_score(y_true,       knn_pred, zero_division=0)
# Evaluasi LR terhadap ground_truth
acc_lr    = accuracy_score(y_true,  lr_pred)
prec_lr   = precision_score(y_true, lr_pred, zero_division=0)
rec_lr    = recall_score(y_true,   lr_pred, zero_division=0)
f1_lr     = f1_score(y_true,       lr_pred, zero_division=0)
metrics_akhir = {
    "knn": {
        "accuracy":  round(acc_knn,  4),
        "precision": round(prec_knn, 4),
        "recall":    round(rec_knn,  4),
        "f1_score":  round(f1_knn,   4),
    },
    "logistic_regression": {
        "accuracy":  round(acc_lr,  4),
        "precision": round(prec_lr, 4),
        "recall":    round(rec_lr,  4),
        "f1_score":  round(f1_lr,   4),
    }
}

print(f"  {'Metrik':<14} {'kNN':>12} {'LR':>12}")
print(f"  {'─'*14} {'─'*12} {'─'*12}")
for m in ["accuracy", "precision", "recall", "f1_score"]:
    kv = metrics_akhir["knn"][m]
    lv = metrics_akhir["logistic_regression"][m]
    print(f"  {m:<14} {kv:>12.4f} {lv:>12.4f}")
# Simpan ke JSON
with open(out("metrics_akhir_streaming.json"), "w", encoding="utf-8") as f:
    json.dump(metrics_akhir, f, indent=2, ensure_ascii=False)

# ============================================================
# PERBANDINGAN EVALUASI AWAL vs AKHIR
# ============================================================
print("\n" + "=" * 65)
print("Langkah 5: Perbandingan Evaluasi Metrik Awal vs Akhir")
print("=" * 65)
print("  awal  = evaluasi offline pada data uji (sebelum streaming)")
print("  akhir = evaluasi pada data hasil streaming postgresql")

metrik_labels = ["Accuracy", "Precision", "Recall", "F1-Score"]
metrik_keys   = ["accuracy", "precision", "recall", "f1_score"]

if metrics_awal:
    print()
    w_metrik = 14
    w_col    = 12
    print(f"  {'Metrik':<{w_metrik}} {'kNN Awal':>{w_col}} {'kNN Akhir':>{w_col}} {'LR Awal':>{w_col}} {'LR Akhir':>{w_col}}")
    print(f"  {'─'*w_metrik} {'─'*w_col} {'─'*w_col} {'─'*w_col} {'─'*w_col}")
    for mk, ml in zip(metrik_keys, metrik_labels):
        kv_aw = metrics_awal["knn"][mk]
        kv_ak = metrics_akhir["knn"][mk]
        lv_aw = metrics_awal["logistic_regression"][mk]
        lv_ak = metrics_akhir["logistic_regression"][mk]
        print(f"  {ml:<{w_metrik}} {kv_aw:>{w_col}.4f} {kv_ak:>{w_col}.4f} {lv_aw:>{w_col}.4f} {lv_ak:>{w_col}.4f}")
    x     = np.arange(len(metrik_labels))
    bar_w = 0.2
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for ax, model_key, model_nama, color_aw, color_ak in [
        (axes[0], "knn",                 "kNN",                "#4C9BE8", "#1A5296"),
        (axes[1], "logistic_regression", "Logistic Regression", "#5DBE6E", "#1E6E34"),
    ]:
        vals_awal  = [metrics_awal[model_key][mk] for mk in metrik_keys]
        vals_akhir = [metrics_akhir[model_key][mk] for mk in metrik_keys]
        bars_aw = ax.bar(x - bar_w/2, vals_awal,  bar_w, label="Awal (Offline)",  color=color_aw,  edgecolor="white", alpha=0.85)
        bars_ak = ax.bar(x + bar_w/2, vals_akhir, bar_w, label="Akhir (Streaming)", color=color_ak, edgecolor="white", alpha=0.85)
        for bars in [bars_aw, bars_ak]:
            for bar in bars:
                h = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2, h + 0.005,
                        f"{h:.4f}", ha="center", va="bottom", fontsize=7.5, fontweight="bold")
        ax.set_xticks(x); ax.set_xticklabels(metrik_labels, fontsize=10)
        ax.set_ylim(0, 1.15)
        ax.set_ylabel("Nilai Metrik", fontsize=11)
        ax.set_title(f"Perbandingan Metrik Evaluasi — {model_nama}\n(Awal vs Akhir Streaming)",
                     fontsize=12, fontweight="bold")
        ax.legend(fontsize=9)
        ax.grid(axis="y", alpha=0.3)
        ax.axhline(0.9, color="red", linestyle="--", alpha=0.4, label="Threshold 90%")
    plt.suptitle("Perbandingan Evaluasi Metrik: Offline (Awal) vs Streaming (Akhir)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(out("perbandingan_evaluasi_awal_akhir.png"), dpi=150, bbox_inches="tight")
    plt.close()
else:
    print("  ⚠️  perbandingan tidak bisa dibuat karena metrics_awal.json tidak ditemukan.")

# ============================================================
# ANALISIS LATENSI
# ============================================================
print("\n" + "=" * 65)
print(f"Langkah 6: Analisis Latensi — WINDOW_SIZE = {WINDOW_SIZE} event")
print("=" * 65)
print(f"  total event yang dianalisis  : {len(df):,}")
print()

lat = df["latency_ms"].dropna()
lat_mean   = round(lat.mean(),         3)
lat_median = round(lat.median(),       3)
lat_std    = round(lat.std(),          3)
lat_min    = round(lat.min(),          3)
lat_max    = round(lat.max(),          3)
lat_p95    = round(lat.quantile(0.95), 3)
lat_p99    = round(lat.quantile(0.99), 3)
kat_lat = kategori_latency(lat_mean)
df_deskriptif = pd.DataFrame([{
    "Ukuran Window (event)": WINDOW_SIZE,
    "Rata-rata (ms)":        lat_mean,
    "Median (ms)":           lat_median,
    "Minimum (ms)":          lat_min,
    "Maksimum (ms)":         lat_max,
    "Std. Deviasi (ms)":     lat_std,
    "Persentil 95 (ms)":     lat_p95,
    "Persentil 99 (ms)":     lat_p99,
    "Kategori":              kat_lat,
}])

print(f"  {'Statistik':<25} {'Nilai':>15}")
print(f"  {'─'*25} {'─'*15}")
for col, val in df_deskriptif.T.iterrows():
    print(f"  {col:<25} {str(val.values[0]):>15}")
df_deskriptif.to_csv(out("analisis_deskriptif_latensi.csv"), index=False)
with open(out("analisis_deskriptif_latensi.json"), "w", encoding="utf-8") as f:
    json.dump(df_deskriptif.to_dict(orient="records"), f, indent=2, ensure_ascii=False)
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
lat_plot = lat[lat < lat.quantile(0.99)]
axes[0].hist(lat_plot, bins=40, color="mediumpurple", edgecolor="white", alpha=0.85)
axes[0].axvline(lat_mean,   color="red",        linestyle="--", linewidth=1.5, label=f"Mean = {lat_mean:.3f} ms")
axes[0].axvline(lat_median, color="darkorange",  linestyle="--", linewidth=1.5, label=f"Median = {lat_median:.3f} ms")
axes[0].axvline(1000,        color="gray",        linestyle=":",  linewidth=1.2, label="Threshold 1.000 ms")
axes[0].set_xlabel("Latency (ms)", fontsize=11)
axes[0].set_ylabel("Frekuensi", fontsize=11)
axes[0].set_title(f"Distribusi Latensi per Event\n(Window Size = {WINDOW_SIZE})", fontsize=12, fontweight="bold")
axes[0].legend(fontsize=9)
axes[0].grid(axis="y", alpha=0.3)
lat_per_window = df.groupby("window_id")["latency_ms"].mean()
axes[1].plot(lat_per_window.index, lat_per_window.values, color="darkorange", marker="o", markersize=4, linewidth=1.5)
axes[1].axhline(lat_mean, color="red",  linestyle="--", alpha=0.7, label=f"Mean keseluruhan = {lat_mean:.3f} ms")
axes[1].axhline(1000,     color="gray", linestyle=":",  alpha=0.5, label="Threshold 1.000 ms")
axes[1].set_xlabel("Window ke-", fontsize=11)
axes[1].set_ylabel("Rata-rata Latensi (ms)", fontsize=11)
axes[1].set_title(f"Rata-rata Latensi per Window\n(Window Size = {WINDOW_SIZE})", fontsize=12, fontweight="bold")
axes[1].legend(fontsize=9)
axes[1].grid(alpha=0.3)
plt.suptitle(f"Analisis Latensi Pipeline Streaming Analytics — Window Size = {WINDOW_SIZE}", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig(out("analisis_latency.png"), dpi=150, bbox_inches="tight")
plt.close()

# ============================================================
# ANALISIS KOMBINASI PREDIKSI kNN vs LR
# ============================================================
print("\n" + "=" * 65)
print("Langkah 7: Analisis Kombinasi Prediksi kNN vs LR")
print("=" * 65)

df["kombinasi"] = "kNN=" + df["label_knn"] + " | LR=" + df["label_lr"]
kombinasi_counts = df["kombinasi"].value_counts().reset_index()
kombinasi_counts.columns = ["Kombinasi", "Jumlah"]
kombinasi_counts["Persentase (%)"] = (kombinasi_counts["Jumlah"] / len(df) * 100).round(2)
kombinasi_counts["Persentase_str"] = kombinasi_counts["Persentase (%)"].map(lambda x: f"{x:.2f}%")
urutan = [
    "kNN=normal | LR=normal",
    "kNN=normal | LR=anomali",
    "kNN=anomali | LR=normal",
    "kNN=anomali | LR=anomali",
]
for k in urutan:
    if k not in kombinasi_counts["Kombinasi"].values:
        kombinasi_counts = pd.concat([
            kombinasi_counts,
            pd.DataFrame([{"Kombinasi": k, "Jumlah": 0, "Persentase (%)": 0.0, "Persentase_str": "0.00%"}])
        ], ignore_index=True)
kombinasi_counts = kombinasi_counts.set_index("Kombinasi").reindex(urutan).reset_index()

print(f"  total event : {len(df):,}")
print()

# Tabel kombinasi — lebar kolom dinamis
w_komb = max((len(k) for k in urutan), default=9)
w_komb = max(w_komb, len("Kombinasi"))
w_jml2 = max(len(f"{kombinasi_counts['Jumlah'].max():,}"), len("Jumlah"))
w_pct2 = 10

print(f"  {'Kombinasi':<{w_komb}}  {'Jumlah':>{w_jml2}}  {'Persentase':>{w_pct2}}")
print(f"  {'─'*w_komb}  {'─'*w_jml2}  {'─'*w_pct2}")
for _, row in kombinasi_counts.iterrows():
    print(f"  {row['Kombinasi']:<{w_komb}}  {row['Jumlah']:>{w_jml2},}  {row['Persentase_str']:>{w_pct2}}")

nn = kombinasi_counts.loc[kombinasi_counts["Kombinasi"]=="kNN=normal | LR=normal",   "Persentase (%)"].values[0]
na = kombinasi_counts.loc[kombinasi_counts["Kombinasi"]=="kNN=normal | LR=anomali",  "Persentase (%)"].values[0]
an = kombinasi_counts.loc[kombinasi_counts["Kombinasi"]=="kNN=anomali | LR=normal",  "Persentase (%)"].values[0]
aa = kombinasi_counts.loc[kombinasi_counts["Kombinasi"]=="kNN=anomali | LR=anomali", "Persentase (%)"].values[0]

w_interp = 42
print()
print(f"  interpretasi:")
print(f"  {'kedua model sepakat normal  (kNN=N, LR=N)':<{w_interp}}: {nn:.2f}%")
print(f"  {'kedua model sepakat anomali (kNN=A, LR=A)':<{w_interp}}: {aa:.2f}%")
print(f"  {'hanya LR deteksi anomali   (kNN=N, LR=A)':<{w_interp}}: {na:.2f}%")
print(f"  {'hanya kNN deteksi anomali  (kNN=A, LR=N)':<{w_interp}}: {an:.2f}%")
print(f"  {'tingkat kesepakatan total':<{w_interp}}: {nn + aa:.2f}%")
print(f"  {'tingkat ketidaksepakatan':<{w_interp}}: {na + an:.2f}%")

df_agregat = kombinasi_counts[["Kombinasi", "Jumlah", "Persentase (%)"]].copy()
df_agregat["Kategori"] = [
    "kedua model sepakat: normal",
    "hanya LR deteksi anomali",
    "hanya kNN deteksi anomali",
    "kedua model sepakat: anomali",
]
df_agregat["Makna"] = [
    "trafik aman (konsensus normal)",
    "LR lebih sensitif — perlu investigasi",
    "kNN lebih sensitif — perlu investigasi",
    "trafik berbahaya (konsensus anomali)",
]

# Tabel agregat rapi — lebar kolom dinamis
print()
print("\n" + "=" * 65)
print("Tabel Statistik Agregat Kombinasi Prediksi")
print("=" * 65)

w_komb2 = max((len(str(v)) for v in df_agregat["Kombinasi"]), default=9);  w_komb2 = max(w_komb2, len("Kombinasi"))
w_jml3  = max((len(f"{v:,}") for v in df_agregat["Jumlah"]),  default=6);  w_jml3  = max(w_jml3,  len("Jumlah"))
w_pct3  = max((len(f"{v:.2f}") for v in df_agregat["Persentase (%)"]), default=14); w_pct3 = max(w_pct3, len("Persentase (%)"))
w_kat   = max((len(str(v)) for v in df_agregat["Kategori"]),  default=8);  w_kat   = max(w_kat,   len("Kategori"))
w_makna = max((len(str(v)) for v in df_agregat["Makna"]),     default=5);  w_makna = max(w_makna, len("Makna"))

print(f"  {'Kombinasi':<{w_komb2}}  {'Jumlah':>{w_jml3}}  {'Persentase (%)':>{w_pct3}}  {'Kategori':<{w_kat}}  {'Makna':<{w_makna}}")
print(f"  {'─'*w_komb2}  {'─'*w_jml3}  {'─'*w_pct3}  {'─'*w_kat}  {'─'*w_makna}")
for _, row in df_agregat.iterrows():
    print(f"  {str(row['Kombinasi']):<{w_komb2}}  {row['Jumlah']:>{w_jml3},}  {row['Persentase (%)']:>{w_pct3}.2f}  {str(row['Kategori']):<{w_kat}}  {str(row['Makna']):<{w_makna}}")

df_agregat.to_csv(out("statistik_agregat_kombinasi.csv"), index=False)
with open(out("statistik_agregat_kombinasi.json"), "w", encoding="utf-8") as f:
    json.dump(df_agregat.to_dict(orient="records"), f, indent=2, ensure_ascii=False)
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
label_pendek = ["kNN=normal\nLR=normal", "kNN=normal\nLR=anomali", "kNN=anomali\nLR=normal", "kNN=anomali\nLR=anomali"]
pct_values = kombinasi_counts["Persentase (%)"].values
jml_values = kombinasi_counts["Jumlah"].values
bar_colors  = ["#4C9BE8", "#F4A460", "#F4A460", "#E85C5C"]
bars = axes[0].bar(label_pendek, pct_values, color=bar_colors, edgecolor="white", linewidth=1.2, width=0.55)
axes[0].set_ylabel("Persentase (%)", fontsize=11)
axes[0].set_title("Statistik Agregat Kombinasi Prediksi\nkNN vs Logistic Regression", fontsize=12, fontweight="bold")
axes[0].grid(axis="y", alpha=0.3)

for bar, pct, jml in zip(bars, pct_values, jml_values):
    axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(pct_values) * 0.02,
                 f"{pct:.2f}%\n({jml:,})", ha="center", va="bottom", fontsize=9, fontweight="bold")
label_pie = [f"kNN=N, LR=N\n{pct_values[0]:.2f}%", f"kNN=N, LR=A\n{pct_values[1]:.2f}%",
             f"kNN=A, LR=N\n{pct_values[2]:.2f}%", f"kNN=A, LR=A\n{pct_values[3]:.2f}%"]
wedges, texts, autotexts = axes[1].pie(pct_values, labels=label_pie, autopct="%1.2f%%", startangle=140,
    colors=bar_colors, wedgeprops={"edgecolor": "white", "linewidth": 1.5}, textprops={"fontsize": 9})

for at in autotexts:
    at.set_fontsize(8); at.set_fontweight("bold")
axes[1].set_title("Proporsi Kombinasi Prediksi\n(Pie Chart)", fontsize=12, fontweight="bold")

plt.suptitle("Analisis Statistik Agregat: Kombinasi Hasil Prediksi kNN vs LR", fontsize=13, fontweight="bold", y=1.01)
plt.tight_layout()
plt.savefig(out("statistik_agregat_kombinasi.png"), dpi=150, bbox_inches="tight")
plt.close()

# ============================================================
# TABEL RINGKASAN EVALUASI AKHIR
# ============================================================
print("\n" + "=" * 65)
print("Langkah 8: Tabel Ringkasan Evaluasi Akhir")
print("=" * 65)
print(f"  berdasarkan {len(df):,} event hasil streaming (window_size={WINDOW_SIZE})")
print()

ringkasan_rows = []

for mk, ml in zip(metrik_keys, metrik_labels):
    kv_aw = metrics_awal["knn"][mk]                 if metrics_awal else 0.0
    lv_aw = metrics_awal["logistic_regression"][mk] if metrics_awal else 0.0
    kv_ak = metrics_akhir["knn"][mk]
    lv_ak = metrics_akhir["logistic_regression"][mk]
    ringkasan_rows.append({
        "Metrik":      ml,
        "kNN (Awal)":  f"{kv_aw:.4f} ({kat_metrik(kv_aw)})",
        "kNN (Akhir)": f"{kv_ak:.4f} ({kat_metrik(kv_ak)})",
        "LR (Awal)":   f"{lv_aw:.4f} ({kat_metrik(lv_aw)})",
        "LR (Akhir)":  f"{lv_ak:.4f} ({kat_metrik(lv_ak)})",
    })

ringkasan_rows.append({
    "Metrik":      "Latency Rata-rata",
    "kNN (Awal)":  "—",
    "kNN (Akhir)": f"{lat_mean} ms ({kat_lat})",
    "LR (Awal)":   "—",
    "LR (Akhir)":  f"{lat_mean} ms ({kat_lat})",
})

df_ringkasan = pd.DataFrame(ringkasan_rows)

# Tabel ringkasan rapi — lebar kolom dinamis
col_widths = {
    col: max(len(col), df_ringkasan[col].astype(str).map(len).max())
    for col in df_ringkasan.columns
}

header = "  " + "  ".join(f"{col:<{col_widths[col]}}" for col in df_ringkasan.columns)
sep    = "  " + "  ".join("─" * col_widths[col] for col in df_ringkasan.columns)
print(header)
print(sep)

for _, row in df_ringkasan.iterrows():
    print("  " + "  ".join(f"{str(row[col]):<{col_widths[col]}}" for col in df_ringkasan.columns))
print()

df_ringkasan.to_csv(out("tabel_evaluasi_akhir.csv"), index=False)
ringkasan_json = {
    "total_event_streaming": len(df),
    "window_size":     WINDOW_SIZE,
    "evaluasi_awal":         metrics_awal if metrics_awal else {},
    "evaluasi_akhir":        metrics_akhir,
    "latensi_streaming": {
        "window_size":   WINDOW_SIZE,
        "mean_ms":       lat_mean,
        "median_ms":     lat_median,
        "std_ms":        lat_std,
        "min_ms":        lat_min,
        "max_ms":        lat_max,
        "p95_ms":        lat_p95,
        "p99_ms":        lat_p99,
        "kategori":      kat_lat,
    },
}

with open(out("ringkasan_evaluasi_lengkap.json"), "w", encoding="utf-8") as f:
    json.dump(ringkasan_json, f, indent=2, ensure_ascii=False)

print("\n" + "=" * 65)
print("EVALUASI SELESAI — RINGKASAN")
print("=" * 65)
print(f"  total event streaming yang dianalisis : {len(df):,}")
print(f"  konfigurasi window                    : {WINDOW_SIZE} event per window")
print(f"  latensi rata-rata                     : {lat_mean} ms → {kat_lat}")
print(f"  kNN  — accuracy Akhir : {metrics_akhir['knn']['accuracy']:.4f} | "
      f"f1-score: {metrics_akhir['knn']['f1_score']:.4f}")
print(f"  LR   — accuracy Akhir : {metrics_akhir['logistic_regression']['accuracy']:.4f} | "
      f"f1-score: {metrics_akhir['logistic_regression']['f1_score']:.4f}\n")