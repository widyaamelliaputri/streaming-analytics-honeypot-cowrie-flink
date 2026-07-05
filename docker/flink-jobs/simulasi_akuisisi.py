import os, sys, json, time, logging
from datetime import datetime

ENV_NAME  = "CONTAINER (Flink/Linux)"
LOG_FILE  = "/opt/flink/log/simulasi_akuisisi.txt"

from pymongo import MongoClient
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Simulasi-Akuisisi] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ============================================================
# KONFIGURASI — dibaca dari environment variables (.env)
# ============================================================
MONGO_URI  = os.environ.get("MONGODB_CONNECTION_STRING", "mongodb://localhost:27017/")
MONGO_DB   = os.environ.get("MONGODB_DATABASE",   "hp_upnvj_1")
MONGO_COL  = os.environ.get("MONGODB_COLLECTION", "cowrie")

# Jumlah dokumen per windowing (dapat disesuaikan untuk pengujian)
WINDOWING_SIZE = 10_000

# Delay antar putaran (detik); set 0 untuk satu kali jalan
REPEAT_DELAY_SECONDS = 0

# ============================================================
# HELPER — serialisasi dokumen MongoDB ke dict JSON-safe
# ============================================================
def serialize_doc(doc: dict) -> dict:
    result = {}
    for k, v in doc.items():
        if k == "_id":
            continue
        elif hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        elif hasattr(v, "__float__"):
            try:    result[k] = float(v)
            except: result[k] = None
        elif isinstance(v, (int, float, str, bool, type(None))):
            result[k] = v
        else:
            result[k] = str(v)
    return result

# ============================================================
# FETCH MONGODB WINDOWING
# ============================================================
def fetch_mongo_window(skip: int, limit: int) -> list:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    col    = client[MONGO_DB][MONGO_COL]
    cursor = col.find({}, {
        "eventid": 1, "src_ip": 1, "src_port": 1,
        "dst_ip": 1, "dst_port": 1, "session": 1,
        "protocol": 1, "timestamp": 1, "duration": 1,
        "username": 1, "password": 1, "input": 1,
        "url": 1, "outfile": 1, "shasum": 1,
    }).skip(skip).limit(limit).batch_size(1000)

    json_strings = []
    for raw_doc in cursor:
        doc = serialize_doc(raw_doc)
        json_strings.append(json.dumps(doc, default=str))
    client.close()
    return json_strings

def get_total_docs() -> int:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    col    = client[MONGO_DB][MONGO_COL]
    total  = col.count_documents({})
    client.close()
    return total

# ============================================================
# MAP FUNCTION — validasi dan pencatatan event (tanpa klasifikasi)
# ============================================================
class AkuisisiMapFunction(MapFunction):
    """
    MapFunction untuk tahap simulasi akuisisi data.
    Tidak melakukan klasifikasi — hanya memvalidasi struktur
    dokumen yang diterima dan mencatat statistik per event.
    """

    # Field wajib yang diharapkan ada di setiap dokumen Cowrie
    REQUIRED_FIELDS = ["eventid", "src_ip", "session", "timestamp"]

    def __init__(self):
        self._event_count  = 0
        self._error_count  = 0
        self._field_stats  = {}   # {field_name: jumlah_hadir}
        self._eventid_dist = {}   # {eventid: count}

    def map(self, json_str: str) -> str:
        self._event_count += 1

        # --- Parse JSON ---
        try:
            doc = json.loads(json_str)
        except Exception as e:
            self._error_count += 1
            return json.dumps({
                "status": "ERROR",
                "event":  self._event_count,
                "reason": f"Gagal parse JSON: {e}",
            })

        # --- Validasi field wajib ---
        missing = [f for f in self.REQUIRED_FIELDS if not doc.get(f)]
        if missing:
            self._error_count += 1
            return json.dumps({
                "status":  "WARNING",
                "event":   self._event_count,
                "reason":  f"Field tidak lengkap: {missing}",
                "src_ip":  doc.get("src_ip", "-"),
                "eventid": doc.get("eventid", "-"),
            })

        # --- Catat distribusi eventid ---
        eid = str(doc.get("eventid", "unknown"))
        self._eventid_dist[eid] = self._eventid_dist.get(eid, 0) + 1

        # --- Catat kehadiran field opsional ---
        for field in ["protocol", "duration", "username", "password",
                      "input", "url", "outfile", "shasum"]:
            if doc.get(field) is not None and doc.get(field) != "":
                self._field_stats[field] = self._field_stats.get(field, 0) + 1

        # --- Log setiap 5.000 event ---
        if self._event_count % 1 == 0:
            log.info(
                f"  [MapFn] ✅ {self._event_count:,} event diproses | "
                f"error={self._error_count:,}"
            )

        return json.dumps({
            "status":    "OK",
            "event":     self._event_count,
            "session":   str(doc.get("session", "-")),
            "src_ip":    str(doc.get("src_ip", "-")),
            "eventid":   eid,
            "timestamp": str(doc.get("timestamp", "-")),
            "has_proto": bool(doc.get("protocol")),
            "has_input": bool(doc.get("input")),
            "has_url":   bool(doc.get("url")),
        })

    def close(self):
        log.info(
            f"\n  [MapFn] ── Ringkasan Task ──────────────────────────────\n"
            f"  Total event masuk    : {self._event_count:,}\n"
            f"  Error / warning      : {self._error_count:,}\n"
            f"  Distribusi eventid   :\n" +
            "".join(
                f"    {k:<40} : {v:,}\n"
                for k, v in sorted(
                    self._eventid_dist.items(), key=lambda x: -x[1]
                )
            ) +
            f"  Kehadiran field opsional:\n" +
            "".join(
                f"    {k:<20} : {v:,} event\n"
                for k, v in sorted(self._field_stats.items())
            )
        )

# ============================================================
# JALANKAN SATU WINDOWING KE FLINK
# ============================================================
def jalankan_flink_job(json_strings: list, label: str) -> list:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.from_collection(
        collection=json_strings,
        type_info=Types.STRING()
    )

    result_ds = ds.map(AkuisisiMapFunction(), output_type=Types.STRING())

    log.info(f"  Submit job: {label} ...")
    results = []
    with result_ds.execute_and_collect(label) as it:
        for r in it:
            results.append(r)

    log.info(f"  ✅ Job selesai. {len(results):,} hasil.")
    return results

# ============================================================
# CETAK RINGKASAN WINDOWING
# ============================================================
def cetak_ringkasan_windowing(results: list, window_num: int):
    ok      = sum(1 for r in results if _status(r) == "OK")
    warn    = sum(1 for r in results if _status(r) == "WARNING")
    err     = sum(1 for r in results if _status(r) == "ERROR")
    log.info(
        f"  📦 Windowing #{window_num} → "
        f"OK={ok:,} | WARNING={warn:,} | ERROR={err:,}"
    )

def _status(r: str) -> str:
    try:
        return json.loads(r).get("status", "ERROR")
    except Exception:
        return "ERROR"

# ============================================================
# CETAK RINGKASAN AKHIR PUTARAN
# ============================================================
def cetak_ringkasan_putaran(all_results: list, putaran: int, total_docs: int):
    ok   = sum(1 for r in all_results if _status(r) == "OK")
    warn = sum(1 for r in all_results if _status(r) == "WARNING")
    err  = sum(1 for r in all_results if _status(r) == "ERROR")

    # Hitung distribusi eventid dari semua hasil
    eventid_dist = {}
    for r in all_results:
        try:
            d   = json.loads(r)
            eid = d.get("eventid")
            if eid:
                eventid_dist[eid] = eventid_dist.get(eid, 0) + 1
        except Exception:
            pass

    log.info(f"\n{'='*65}")
    log.info(f"  📊 RINGKASAN PUTARAN #{putaran} — SIMULASI AKUISISI DATA")
    log.info(f"{'='*65}")
    log.info(f"  Total dokumen di MongoDB : {total_docs:,}")
    log.info(f"  Total diterima Flink     : {len(all_results):,}")
    log.info(f"  Status OK                : {ok:,}")
    log.info(f"  Status WARNING           : {warn:,}")
    log.info(f"  Status ERROR             : {err:,}")
    log.info(f"  Distribusi eventid:")
    for eid, cnt in sorted(eventid_dist.items(), key=lambda x: -x[1]):
        log.info(f"    {eid:<42} : {cnt:,}")
    log.info(f"{'='*65}\n")

    # Verifikasi kelengkapan transfer
    if len(all_results) == total_docs:
        log.info("  ✅ VERIFIKASI: Semua dokumen berhasil dikirim ke Flink.")
    else:
        selisih = total_docs - len(all_results)
        log.warning(
            f"  ⚠️  VERIFIKASI: {selisih:,} dokumen tidak terkirim / error."
        )

# ============================================================
# MAIN
# ============================================================
def main():
    log.info("\n" + "=" * 65)
    log.info("SIMULASI AKUISISI DATA — MongoDB → Apache Flink")
    log.info(f"  Environment  : {ENV_NAME}")
    log.info(f"  MongoDB      : DB={MONGO_DB} | COL={MONGO_COL}")
    log.info("=" * 65)

    # Verifikasi koneksi MongoDB sebelum memulai
    try:
        total_docs = get_total_docs()
        log.info(f"  ✅ MongoDB terhubung. Total dokumen: {total_docs:,}")
    except Exception as e:
        log.error(f"  ❌ Gagal koneksi MongoDB: {e}")
        sys.exit(1)

    putaran = 0
    while True:
        putaran += 1
        log.info(f"\n{'─'*65}")
        log.info(
            f"  ▶ PUTARAN #{putaran} — "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        log.info(f"{'─'*65}")

        all_results = []
        skip        = 0
        window_num  = 0

        while skip < total_docs:
            window_num += 1
            log.info(
                f"  📦 Windowing #{window_num} | "
                f"offset={skip:,} | "
                f"sisa={max(0, total_docs - skip):,}"
            )

            # --- Fetch dari MongoDB ---
            try:
                t_fetch = time.time()
                json_strings = fetch_mongo_window(skip, WINDOWING_SIZE)
                dur_fetch = round((time.time() - t_fetch) * 1000, 1)
                log.info(
                    f"  ✅ Windowing selesai: {len(json_strings):,} dokumen "
                    f"({dur_fetch:.1f} ms)"
                )
            except Exception as e:
                log.error(f"  ❌ Windowing #{window_num} gagal: {e}")
                time.sleep(5)
                continue

            if not json_strings:
                log.info(f"  Windowing #{window_num} kosong, selesai.")
                break

            # --- Kirim ke Flink ---
            job_label = (
                f"Simulasi Akuisisi — "
                f"Putaran #{putaran} Windowing #{window_num} "
            )
            try:
                t_flink = time.time()
                results = jalankan_flink_job(json_strings, job_label)
                dur_flink = round((time.time() - t_flink) * 1000, 1)
                log.info(
                    f"  ✅ Flink selesai: {len(results):,} hasil "
                    f"({dur_flink:.1f} ms)"
                )
                all_results.extend(results)
                cetak_ringkasan_windowing(results, window_num)
            except Exception as e:
                log.error(f"  ❌ Flink job windowing #{window_num} gagal: {e}")
                time.sleep(5)
                continue

            skip += len(json_strings)

        cetak_ringkasan_putaran(all_results, putaran, total_docs)

        if REPEAT_DELAY_SECONDS > 0:
            log.info(
                f"  ⏳ Jeda {REPEAT_DELAY_SECONDS}s sebelum putaran "
                f"#{putaran + 1}..."
            )
            time.sleep(REPEAT_DELAY_SECONDS)
        else:
            log.info("  ✅ Simulasi akuisisi selesai (satu putaran).")
            break

if __name__ == "__main__":
    main()
