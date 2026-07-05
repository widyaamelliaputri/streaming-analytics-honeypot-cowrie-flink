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
# ANALISIS LATENSI PER MODEL
# ============================================================
print("\n" + "=" * 65)
print(f"Langkah 6: Analisis Latensi per Model (kNN vs Logistic Regression)")
print("=" * 65)
print(f"  total event yang dianalisis  : {len(df):,}")
print()

# Pastikan kolom latency per model tersedia
for col_lat in ["latency_knn_ms", "latency_lr_ms"]:
    if col_lat not in df.columns:
        raise ValueError(
            f"Kolom '{col_lat}' tidak ditemukan di tabel classification_result.\n"
            "Pastikan init.sql sudah menambahkan kolom latency_knn_ms dan latency_lr_ms,\n"
            "dan streaming_job.py sudah diperbarui untuk mengisi kolom tersebut."
        )

lat_knn = df["latency_knn_ms"].dropna()
lat_lr  = df["latency_lr_ms"].dropna()
lat_all = df["latency_ms"].dropna()   # total (kNN + LR + preprocessing)

def statistik_latensi(seri):
    return {
        "mean":   round(seri.mean(),         4),
        "median": round(seri.median(),       4),
        "std":    round(seri.std(),          4),
        "min":    round(seri.min(),          4),
        "max":    round(seri.max(),          4),
        "p95":    round(seri.quantile(0.95), 4),
        "p99":    round(seri.quantile(0.99), 4),
    }

stat_knn = statistik_latensi(lat_knn)
stat_lr  = statistik_latensi(lat_lr)
stat_all = statistik_latensi(lat_all)

# Tetap simpan variabel lat_mean untuk ringkasan di bawah
lat_mean   = stat_all["mean"]
lat_median = stat_all["median"]
lat_std    = stat_all["std"]
lat_min    = stat_all["min"]
lat_max    = stat_all["max"]
lat_p95    = stat_all["p95"]
lat_p99    = stat_all["p99"]
kat_lat    = kategori_latency(lat_mean)

print(f"  {'Statistik':<25} {'kNN (ms)':>14} {'LR (ms)':>14} {'Total (ms)':>14}")
print(f"  {'─'*25} {'─'*14} {'─'*14} {'─'*14}")
for k, label in [
    ("mean",   "Rata-rata"),
    ("median", "Median"),
    ("std",    "Std. Deviasi"),
    ("min",    "Minimum"),
    ("max",    "Maksimum"),
    ("p95",    "Persentil 95"),
    ("p99",    "Persentil 99"),
]:
    print(f"  {label:<25} {stat_knn[k]:>14.4f} {stat_lr[k]:>14.4f} {stat_all[k]:>14.4f}")

df_deskriptif = pd.DataFrame([{
    "Model":                "kNN",
    "Rata-rata (ms)":       stat_knn["mean"],
    "Median (ms)":          stat_knn["median"],
    "Std. Deviasi (ms)":    stat_knn["std"],
    "Minimum (ms)":         stat_knn["min"],
    "Maksimum (ms)":        stat_knn["max"],
    "Persentil 95 (ms)":    stat_knn["p95"],
    "Persentil 99 (ms)":    stat_knn["p99"],
}, {
    "Model":                "Logistic Regression",
    "Rata-rata (ms)":       stat_lr["mean"],
    "Median (ms)":          stat_lr["median"],
    "Std. Deviasi (ms)":    stat_lr["std"],
    "Minimum (ms)":         stat_lr["min"],
    "Maksimum (ms)":        stat_lr["max"],
    "Persentil 95 (ms)":    stat_lr["p95"],
    "Persentil 99 (ms)":    stat_lr["p99"],
}])
df_deskriptif.to_csv(out("analisis_deskriptif_latensi.csv"), index=False)
with open(out("analisis_deskriptif_latensi.json"), "w", encoding="utf-8") as f:
    json.dump(df_deskriptif.to_dict(orient="records"), f, indent=2, ensure_ascii=False)

# Visualisasi distribusi latensi per model
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

for ax, seri, nama, color, mean_val in [
    (axes[0], lat_knn, "kNN",                 "steelblue", stat_knn["mean"]),
    (axes[1], lat_lr,  "Logistic Regression", "seagreen",  stat_lr["mean"]),
]:
    q99 = seri.quantile(0.99)
    seri_plot = seri[seri < q99]
    ax.hist(seri_plot, bins=40, color=color, edgecolor="white", alpha=0.85)
    ax.axvline(mean_val, color="red",       linestyle="--", linewidth=1.5, label=f"Mean = {mean_val:.4f} ms")
    ax.axvline(seri.median(), color="darkorange", linestyle="--", linewidth=1.5, label=f"Median = {seri.median():.4f} ms")
    ax.set_xlabel("Latensi (ms)", fontsize=11)
    ax.set_ylabel("Frekuensi", fontsize=11)
    ax.set_title(f"Distribusi Latensi — {nama}", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)

# Boxplot perbandingan kNN vs LR
axes[2].boxplot(
    [lat_knn.values, lat_lr.values],
    labels=["kNN", "Logistic Regression"],
    patch_artist=True,
    boxprops=dict(facecolor="lightyellow", color="gray"),
    medianprops=dict(color="red", linewidth=2),
    flierprops=dict(marker="o", markersize=2, alpha=0.3),
)
axes[2].set_ylabel("Latensi (ms)", fontsize=11)
axes[2].set_title("Perbandingan Latensi\nkNN vs Logistic Regression", fontsize=12, fontweight="bold")
axes[2].grid(axis="y", alpha=0.3)

plt.suptitle("Analisis Latensi Klasifikasi per Model — Streaming Analytics", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig(out("analisis_latency.png"), dpi=150, bbox_inches="tight")
plt.close()

# ============================================================
# PENGUJIAN HIPOTESIS — ANALISIS DESKRIPTIF LATENSI
# ============================================================
print("\n" + "=" * 65)
print("Langkah 7: Pengujian Hipotesis — Analisis Deskriptif Latensi")
print("=" * 65)
print("  H₀ : Tidak ada perbedaan signifikan pada latensi rata-rata")
print("       klasifikasi per event antara model kNN dan Logistic Regression.")
print("  H₁ : Model kNN menghasilkan latensi rata-rata klasifikasi per event")
print("       yang lebih tinggi dibandingkan model Logistic Regression.")
print()
print("  Pendekatan : Analisis deskriptif — perbandingan nilai rata-rata,")
print("               nilai minimum, nilai maksimum, dan simpangan baku")
print("               latensi klasifikasi pada masing-masing model.")
print()

df_lat_valid = df[["latency_knn_ms", "latency_lr_ms"]].dropna()
n_event      = len(df_lat_valid)

# Hitung statistik deskriptif per model
deskriptif_hipotesis = {
    "kNN": {
        "mean":   round(df_lat_valid["latency_knn_ms"].mean(),  4),
        "min":    round(df_lat_valid["latency_knn_ms"].min(),   4),
        "max":    round(df_lat_valid["latency_knn_ms"].max(),   4),
        "std":    round(df_lat_valid["latency_knn_ms"].std(),   4),
    },
    "Logistic Regression": {
        "mean":   round(df_lat_valid["latency_lr_ms"].mean(),   4),
        "min":    round(df_lat_valid["latency_lr_ms"].min(),    4),
        "max":    round(df_lat_valid["latency_lr_ms"].max(),    4),
        "std":    round(df_lat_valid["latency_lr_ms"].std(),    4),
    },
}

selisih_mean = round(
    deskriptif_hipotesis["kNN"]["mean"] - deskriptif_hipotesis["Logistic Regression"]["mean"], 4
)

# Cetak tabel perbandingan
print(f"  Total event dianalisis : {n_event:,}")
print()
print(f"  {'Statistik':<22} {'kNN (ms)':>14} {'LR (ms)':>14} {'Selisih (ms)':>14}")
print(f"  {'─'*22} {'─'*14} {'─'*14} {'─'*14}")
for k, label in [
    ("mean", "Rata-rata"),
    ("min",  "Minimum"),
    ("max",  "Maksimum"),
    ("std",  "Simpangan Baku"),
]:
    v_knn = deskriptif_hipotesis["kNN"][k]
    v_lr  = deskriptif_hipotesis["Logistic Regression"][k]
    selisih_k = round(v_knn - v_lr, 4)
    print(f"  {label:<22} {v_knn:>14.4f} {v_lr:>14.4f} {selisih_k:>+14.4f}")

print()

# Interpretasi berdasarkan nilai rata-rata (sesuai H₁)
v_mean_knn = deskriptif_hipotesis["kNN"]["mean"]
v_mean_lr  = deskriptif_hipotesis["Logistic Regression"]["mean"]

if v_mean_knn > v_mean_lr:
    kesimpulan_hipotesis = (
        f"Rata-rata latensi kNN ({v_mean_knn:.4f} ms) lebih tinggi dibandingkan "
        f"Logistic Regression ({v_mean_lr:.4f} ms) dengan selisih {abs(selisih_mean):.4f} ms. "
        f"Hasil ini mendukung H₁."
    )
    keputusan_hipotesis = "H₁ didukung"
elif v_mean_knn < v_mean_lr:
    kesimpulan_hipotesis = (
        f"Rata-rata latensi kNN ({v_mean_knn:.4f} ms) lebih rendah dibandingkan "
        f"Logistic Regression ({v_mean_lr:.4f} ms) dengan selisih {abs(selisih_mean):.4f} ms. "
        f"Hasil ini mendukung H₀."
    )
    keputusan_hipotesis = "H₀ didukung"
else:
    kesimpulan_hipotesis = (
        f"Rata-rata latensi kNN dan Logistic Regression identik ({v_mean_knn:.4f} ms). "
        f"Tidak terdapat perbedaan latensi antarmodel."
    )
    keputusan_hipotesis = "Tidak ada perbedaan"

print(f"  Kesimpulan : {kesimpulan_hipotesis}")
print()

# Simpan hasil ke JSON
hasil_uji = {
    "pendekatan":             "Analisis Deskriptif",
    "n_event":                n_event,
    "kNN": {
        "mean_ms": deskriptif_hipotesis["kNN"]["mean"],
        "min_ms":  deskriptif_hipotesis["kNN"]["min"],
        "max_ms":  deskriptif_hipotesis["kNN"]["max"],
        "std_ms":  deskriptif_hipotesis["kNN"]["std"],
    },
    "logistic_regression": {
        "mean_ms": deskriptif_hipotesis["Logistic Regression"]["mean"],
        "min_ms":  deskriptif_hipotesis["Logistic Regression"]["min"],
        "max_ms":  deskriptif_hipotesis["Logistic Regression"]["max"],
        "std_ms":  deskriptif_hipotesis["Logistic Regression"]["std"],
    },
    "selisih_mean_ms":        selisih_mean,
    "keputusan":              keputusan_hipotesis,
    "kesimpulan":             kesimpulan_hipotesis,
}
with open(out("hasil_uji_hipotesis_latensi.json"), "w", encoding="utf-8") as f:
    json.dump(hasil_uji, f, indent=2, ensure_ascii=False)

# Visualisasi perbandingan deskriptif
bar_colors = ["steelblue", "seagreen"]
fig, axes  = plt.subplots(1, 2, figsize=(14, 5))

# Panel kiri — barplot rata-rata ± simpangan baku
means = [v_mean_knn, v_mean_lr]
stds  = [deskriptif_hipotesis["kNN"]["std"],
         deskriptif_hipotesis["Logistic Regression"]["std"]]
bars  = axes[0].bar(
    ["kNN", "Logistic Regression"], means,
    color=bar_colors, edgecolor="white", linewidth=1.2, width=0.5,
    yerr=stds, capsize=6, error_kw={"elinewidth": 1.5, "ecolor": "gray"},
)
for bar, val in zip(bars, means):
    axes[0].text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + max(stds) * 0.15,
        f"{val:.4f} ms", ha="center", va="bottom", fontsize=10, fontweight="bold"
    )
axes[0].set_ylabel("Rata-rata Latensi (ms)", fontsize=11)
axes[0].set_title(
    "Perbandingan Rata-rata Latensi\n(± Simpangan Baku)",
    fontsize=12, fontweight="bold"
)
axes[0].grid(axis="y", alpha=0.3)

# Panel kanan — grouped barplot semua metrik deskriptif
metrik_bar   = ["Rata-rata", "Minimum", "Maksimum", "Simpangan Baku"]
metrik_keys2 = ["mean", "min", "max", "std"]
x2     = np.arange(len(metrik_bar))
bar_w2 = 0.35
vals_knn = [deskriptif_hipotesis["kNN"][k]                  for k in metrik_keys2]
vals_lr  = [deskriptif_hipotesis["Logistic Regression"][k]  for k in metrik_keys2]

b1 = axes[1].bar(x2 - bar_w2/2, vals_knn, bar_w2, label="kNN",
                  color="steelblue", edgecolor="white", alpha=0.85)
b2 = axes[1].bar(x2 + bar_w2/2, vals_lr,  bar_w2, label="Logistic Regression",
                  color="seagreen",  edgecolor="white", alpha=0.85)
for bars2 in [b1, b2]:
    for bar in bars2:
        h = bar.get_height()
        axes[1].text(
            bar.get_x() + bar.get_width() / 2, h + max(max(vals_knn), max(vals_lr)) * 0.01,
            f"{h:.4f}", ha="center", va="bottom", fontsize=8, fontweight="bold"
        )
axes[1].set_xticks(x2)
axes[1].set_xticklabels(metrik_bar, fontsize=10)
axes[1].set_ylabel("Latensi (ms)", fontsize=11)
axes[1].set_title(
    "Perbandingan Statistik Deskriptif Latensi\nkNN vs Logistic Regression",
    fontsize=12, fontweight="bold"
)
axes[1].legend(fontsize=9)
axes[1].grid(axis="y", alpha=0.3)

plt.suptitle(
    "Pengujian Hipotesis — Analisis Deskriptif Latensi Klasifikasi per Event",
    fontsize=13, fontweight="bold"
)
plt.tight_layout()
plt.savefig(out("uji_hipotesis_latensi.png"), dpi=150, bbox_inches="tight")
plt.close()

# ============================================================
# ANALISIS KOMBINASI PREDIKSI kNN vs LR
# ============================================================
print("\n" + "=" * 65)
print("Langkah 8: Analisis Kombinasi Prediksi kNN vs LR")
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
print("Langkah 9: Tabel Ringkasan Evaluasi Akhir")
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
    "Metrik":      "Latency kNN Rata-rata",
    "kNN (Awal)":  "—",
    "kNN (Akhir)": f"{stat_knn['mean']:.4f} ms",
    "LR (Awal)":   "—",
    "LR (Akhir)":  "—",
})
ringkasan_rows.append({
    "Metrik":      "Latency LR Rata-rata",
    "kNN (Awal)":  "—",
    "kNN (Akhir)": "—",
    "LR (Awal)":   "—",
    "LR (Akhir)":  f"{stat_lr['mean']:.4f} ms",
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
        "knn": {
            "mean_ms":    stat_knn["mean"],
            "median_ms":  stat_knn["median"],
            "std_ms":     stat_knn["std"],
            "min_ms":     stat_knn["min"],
            "max_ms":     stat_knn["max"],
            "p95_ms":     stat_knn["p95"],
            "p99_ms":     stat_knn["p99"],
        },
        "logistic_regression": {
            "mean_ms":    stat_lr["mean"],
            "median_ms":  stat_lr["median"],
            "std_ms":     stat_lr["std"],
            "min_ms":     stat_lr["min"],
            "max_ms":     stat_lr["max"],
            "p95_ms":     stat_lr["p95"],
            "p99_ms":     stat_lr["p99"],
        },
        "total_pipeline": {
            "mean_ms":    lat_mean,
            "median_ms":  lat_median,
            "std_ms":     lat_std,
            "min_ms":     lat_min,
            "max_ms":     lat_max,
            "p95_ms":     lat_p95,
            "p99_ms":     lat_p99,
            "kategori":   kat_lat,
        },
    },
    "uji_hipotesis": hasil_uji,
}

with open(out("ringkasan_evaluasi_lengkap.json"), "w", encoding="utf-8") as f:
    json.dump(ringkasan_json, f, indent=2, ensure_ascii=False)

print("\n" + "=" * 65)
print("EVALUASI SELESAI — RINGKASAN")
print("=" * 65)
print(f"  total event streaming yang dianalisis : {len(df):,}")
print(f"  konfigurasi window                    : {WINDOW_SIZE} event per window")
print(f"  latensi rata-rata kNN                 : {stat_knn['mean']:.4f} ms")
print(f"  latensi rata-rata LR                  : {stat_lr['mean']:.4f} ms")
print(f"  selisih rata-rata (kNN − LR)          : {selisih_mean:+.4f} ms → {keputusan_hipotesis}")
print(f"  kNN  — accuracy Akhir : {metrics_akhir['knn']['accuracy']:.4f} | "
      f"f1-score: {metrics_akhir['knn']['f1_score']:.4f}")
print(f"  LR   — accuracy Akhir : {metrics_akhir['logistic_regression']['accuracy']:.4f} | "
      f"f1-score: {metrics_akhir['logistic_regression']['f1_score']:.4f}\n")