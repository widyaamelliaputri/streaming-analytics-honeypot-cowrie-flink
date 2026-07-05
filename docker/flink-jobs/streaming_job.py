import os, sys, json, time, logging, warnings
warnings.filterwarnings("ignore")

ENV_NAME  = "CONTAINER (Flink)"
MODEL_DIR = "/opt/data/models"
LOG_DIR   = "/opt/data/processed"
LOG_FILE  = "/opt/flink/log/streaming_log.txt"
os.makedirs(LOG_DIR,   exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

import numpy as np
import joblib
import psycopg2
from datetime import datetime, timezone
from pymongo import MongoClient

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common.typeinfo import Types

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Flink-Streaming] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ============================================================
# KONFIGURASI — dibaca dari environment variables (.env)
# ============================================================
MONGO_URI            = os.environ.get("MONGODB_CONNECTION_STRING",
                           "mongodb://localhost:27017/")
MONGO_DB             = os.environ.get("MONGODB_DATABASE",   "hp_upnvj_1")
MONGO_COL            = os.environ.get("MONGODB_COLLECTION", "cowrie")
PG_HOST              = os.environ.get("POSTGRES_HOST",      "postgres-hasil")
PG_PORT              = int(os.environ.get("POSTGRES_PORT",  5432))
PG_DB                = os.environ.get("POSTGRES_DATABASE",  "hasil_klasifikasi")
PG_USER              = os.environ.get("POSTGRES_USER",      "skripsi")
PG_PASSWORD          = os.environ.get("POSTGRES_PASSWORD",  "")
WINDOW_SIZE          = 1
REPEAT_DELAY_SECONDS = 5

# definisi event yang termasuk anomali (harus identik dengan 02_preprocessing.py)
ANOMALI_EVENTS = {
    "cowrie.login.failed",           # brute force gagal
    "cowrie.login.success",          # login berhasil di honeypot = anomali!
    "cowrie.command.input",          # eksekusi perintah shell
    "cowrie.session.file_download",  # unduh file malware
    "cowrie.direct-tcpip.request",   # port forwarding mencurigakan
    "cowrie.session.file_upload",    # upload file ke honeypot
}

# Path file state antar-windowing (di dalam container)
STATE_FILE = "/opt/data/processed/windowing_state.json"

# ============================================================
# SQL
# ============================================================
INSERT_SQL = """
    INSERT INTO classification_result (
        session_id, event_time, src_ip, src_port, dst_port,
        protocol, duration,
        login_attempt_count, login_success_count, cmd_count,
        session_event_count, unique_username_count, unique_password_count,
        ip_total_events, ip_total_sessions,
        ip_total_login_fail, ip_total_login_ok, ip_total_downloads,
        has_command, has_download, has_upload, has_malware,
        label_knn, label_lr, ground_truth, latency_ms,
        latency_knn_ms, latency_lr_ms,
        window_id, window_start, window_end
    ) VALUES (
        %(session_id)s, %(event_time)s, %(src_ip)s, %(src_port)s, %(dst_port)s,
        %(protocol)s, %(duration)s,
        %(login_attempt_count)s, %(login_success_count)s, %(cmd_count)s,
        %(session_event_count)s, %(unique_username_count)s, %(unique_password_count)s,
        %(ip_total_events)s, %(ip_total_sessions)s,
        %(ip_total_login_fail)s, %(ip_total_login_ok)s, %(ip_total_downloads)s,
        %(has_command)s, %(has_download)s, %(has_upload)s, %(has_malware)s,
        %(label_knn)s, %(label_lr)s, %(ground_truth)s, %(latency_ms)s,
        %(latency_knn_ms)s, %(latency_lr_ms)s,
        %(window_id)s, %(window_start)s, %(window_end)s
    ) RETURNING id
"""
ALERT_SQL = """
    INSERT INTO alert_log (result_id, src_ip, label_knn, label_lr)
    VALUES (%(result_id)s, %(src_ip)s, %(label_knn)s, %(label_lr)s)
"""

# ============================================================
# HELPER
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
# STATE PERSISTENCE — simpan/muat state antar windowing ke file JSON
# ============================================================
def simpan_state(state: dict):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"  [State] gagal simpan state: {e}")

def muat_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            log.info(
                f"  [State] ✅ state dimuat dari file: "
                f"event_count={state.get('event_count',0):,}, "
                f"window_id={state.get('window_id',1)}, "
                f"sesi aktif={len(state.get('sess_ctx',{})):,}, "
                f"ip aktif={len(state.get('ip_ctx',{})):,}"
            )
            return state
        except Exception as e:
            log.warning(f"  [State] mulai dari kosong: {e}")
    return {"sess_ctx": {}, "ip_ctx": {}, "event_count": 0, "window_id": 1}

def hapus_state():
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
            log.info("  [State] state windowing sebelumnya dihapus.")
        except Exception as e:
            log.warning(f"  [State] gagal hapus state: {e}")

WINDOWING_SIZE = 10000

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

class KlasifikasiMapFunction(MapFunction):
    STATE_SENTINEL = "__STATE_DUMP__"

    def __init__(self, model_dir: str, fitur_names: list, window_ctx_json: str):
        self.model_dir       = model_dir
        self.fitur_names     = fitur_names
        self.window_ctx_json = window_ctx_json
        self.knn     = None
        self.lr      = None
        self.scaler  = None
        self.le_prot = None
        self._pg_conn = None
        self._pg_cur  = None
        self._sess_ctx:     dict = {}
        self._ip_ctx:       dict = {}
        self._event_count:  int  = 0
        self._window_id:    int  = 1
        self._window_start: datetime = None
        self._total_anomali_knn = 0
        self._total_anomali_lr  = 0
        self._total_latency     = 0.0

    def open(self, runtime_context: RuntimeContext):
        import warnings as _w
        _w.filterwarnings("ignore", category=UserWarning)
        _w.filterwarnings("ignore", message=".*InconsistentVersionWarning.*")
        _w.filterwarnings("ignore", message=".*valid feature names.*")
        import logging as _lg
        _lg.getLogger("sklearn").setLevel(_lg.ERROR)

        self.knn     = joblib.load(os.path.join(self.model_dir, "knn_model.pkl"))
        self.lr      = joblib.load(os.path.join(self.model_dir, "lr_model.pkl"))
        self.scaler  = joblib.load(os.path.join(self.model_dir, "scaler.pkl"))
        self.le_prot = joblib.load(
            os.path.join(self.model_dir, "label_encoder_protocol.pkl"))
        log.info("  [MapFn] model kNN + LR + scaler + encoder dimuat")

        self._pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD, connect_timeout=10,
        )
        self._pg_conn.autocommit = True
        self._pg_cur = self._pg_conn.cursor()
        log.info("  [MapFn] koneksi PostgreSQL berhasil")

        # ── Muat state dari windowing sebelumnya ────────────────────
        ctx = json.loads(self.window_ctx_json)
        self._sess_ctx     = ctx.get("sess_ctx",    {})
        self._ip_ctx       = ctx.get("ip_ctx",      {})
        self._event_count  = ctx.get("event_count", 0)
        self._window_id    = ctx.get("window_id",   1)
        self._window_start = datetime.now(timezone.utc)
        log.info(
            f"  [MapFn] state dimuat: "
            f"event_count={self._event_count}, window_id={self._window_id}, "
            f"sesi={len(self._sess_ctx):,}, IP={len(self._ip_ctx):,}"
        )

    def map(self, json_str: str) -> str:
        import warnings as _w
        _w.filterwarnings("ignore", category=UserWarning)

        if json_str == self.STATE_SENTINEL:
            state_dump = {
                "sess_ctx":    self._sess_ctx,
                "ip_ctx":      self._ip_ctx,
                "event_count": self._event_count,
                "window_id":   self._window_id,
            }
            return json.dumps({"__state_dump__": state_dump})

        try:
            doc = json.loads(json_str)
        except Exception as e:
            return json.dumps({"error": f"parse JSON gagal: {e}"})

        self._event_count += 1

        # cek transisi window
        if self._event_count > 1 and (self._event_count - 1) % WINDOW_SIZE == 0:
            self._window_id   += 1
            self._window_start = datetime.now(timezone.utc)
            log.info(
                f"  [MapFn] ── Window #{self._window_id} dimulai "
                f"(event ke-{self._event_count:,}) ──"
            )

        window_end_ts = datetime.now(timezone.utc)
        self._perbarui_state(doc)

        try:
            result = self._klasifikasi(doc)
        except Exception as e:
            log.warning(f"  Skip event #{self._event_count}: {e}")
            return json.dumps({"error": str(e), "event": self._event_count})

        self._simpan_postgresql(
            result,
            window_id    = self._window_id,
            window_start = self._window_start,
            window_end   = window_end_ts,
        )

        if result["label_knn"] == "anomali": self._total_anomali_knn += 1
        if result["label_lr"]  == "anomali": self._total_anomali_lr  += 1
        self._total_latency += result["latency_ms"]

        is_anomali = (result["label_knn"] == "anomali" or
                      result["label_lr"]  == "anomali")
        if is_anomali:
            log.warning(
                f"  🚨 ANOMALI | event #{self._event_count:>7,} | "
                f"IP={result['src_ip']:<20} | "
                f"kNN={result['label_knn']:<7} | LR={result['label_lr']:<7} | "
                f"lat_knn={result['latency_knn_ms']:>6.3f}ms | "
                f"lat_lr={result['latency_lr_ms']:>6.3f}ms"
            )

        return json.dumps({
            "event":           self._event_count,
            "src_ip":          result["src_ip"],
            "label_knn":       result["label_knn"],
            "label_lr":        result["label_lr"],
            "latency_ms":      result["latency_ms"],
            "latency_knn_ms":  result["latency_knn_ms"],
            "latency_lr_ms":   result["latency_lr_ms"],
            "window_id":       self._window_id,
        })

    # ----------------------------------------------------------
    def _perbarui_state(self, doc: dict):
        sid = str(doc.get("session", ""))
        ip  = str(doc.get("src_ip",  ""))
        eid = str(doc.get("eventid", ""))
        usr = str(doc.get("username", "") or "")
        pwd = str(doc.get("password", "") or "")

        if sid not in self._sess_ctx:
            self._sess_ctx[sid] = {
                "cnt": 0, "login_fail": 0, "login_ok": 0,
                "cmd": 0, "uniq_user": 0, "uniq_pass": 0,
            }
        s = self._sess_ctx[sid]
        s["cnt"] += 1
        if eid == "cowrie.login.failed":  s["login_fail"] += 1
        if eid == "cowrie.login.success": s["login_ok"]   += 1
        if eid == "cowrie.command.input": s["cmd"]        += 1
        if usr: s["uniq_user"] += 1
        if pwd: s["uniq_pass"] += 1

        if ip not in self._ip_ctx:
            self._ip_ctx[ip] = {
                "cnt": 0, "sessions": 0,
                "login_fail": 0, "login_ok": 0, "downloads": 0,
            }
        p = self._ip_ctx[ip]
        p["cnt"] += 1
        if s["cnt"] == 1:         p["sessions"]   += 1
        if eid == "cowrie.login.failed":          p["login_fail"] += 1
        if eid == "cowrie.login.success":         p["login_ok"]   += 1
        if eid == "cowrie.session.file_download": p["downloads"]  += 1

    # ----------------------------------------------------------
    def _klasifikasi(self, doc: dict) -> dict:
        t0 = time.time()
        protocol_raw = str(doc.get("protocol") or "unknown")
        try:
            protocol_enc = float(self.le_prot.transform([protocol_raw])[0])
        except ValueError:
            protocol_enc = 0.0

        sid = str(doc.get("session", ""))
        ip  = str(doc.get("src_ip",  ""))
        eid = str(doc.get("eventid", ""))
        sc  = self._sess_ctx.get(sid, {})
        ic  = self._ip_ctx.get(ip,   {})

        # ground truth: 1 = anomali, 0 = normal — diturunkan dari eventid,
        # harus konsisten dengan ANOMALI_EVENTS di 02_preprocessing.py
        ground_truth = 1 if eid in ANOMALI_EVENTS else 0

        fitur_values = {
            "src_port":              float(doc.get("src_port")  or 0),
            "dst_port":              float(doc.get("dst_port")  or 22),
            "duration":              float(doc.get("duration")  or 0),
            "protocol_encoded":      protocol_enc,
            "login_attempt_count":   float(sc.get("login_fail", 0)),
            "login_success_count":   float(sc.get("login_ok",   0)),
            "cmd_count":             float(sc.get("cmd",        0)),
            "session_event_count":   float(sc.get("cnt",        0)),
            "unique_username_count": float(sc.get("uniq_user",  0)),
            "unique_password_count": float(sc.get("uniq_pass",  0)),
            "ip_total_events":       float(ic.get("cnt",        0)),
            "ip_total_sessions":     float(ic.get("sessions",   0)),
            "ip_total_login_fail":   float(ic.get("login_fail", 0)),
            "ip_total_login_ok":     float(ic.get("login_ok",   0)),
            "ip_total_downloads":    float(ic.get("downloads",  0)),
            "has_command":           1.0 if doc.get("input")   else 0.0,
            "has_download":          1.0 if doc.get("url")     else 0.0,
            "has_upload":            1.0 if doc.get("outfile") else 0.0,
            "has_malware":           1.0 if doc.get("shasum")  else 0.0,
        }

        X        = np.array([fitur_values.get(f, 0.0) for f in self.fitur_names],
                             dtype=float).reshape(1, -1)
        X_scaled  = self.scaler.transform(X)

        # Pengukuran latensi kNN secara terpisah
        t_knn_start = time.time()
        label_knn   = "anomali" if int(self.knn.predict(X_scaled)[0]) == 1 else "normal"
        latency_knn = round((time.time() - t_knn_start) * 1000, 4)

        # Pengukuran latensi LR secara terpisah
        t_lr_start = time.time()
        label_lr   = "anomali" if int(self.lr.predict(X_scaled)[0])  == 1 else "normal"
        latency_lr = round((time.time() - t_lr_start) * 1000, 4)

        # Total latensi klasifikasi (kNN + LR) — tidak termasuk feature engineering
        latency    = round((time.time() - t0) * 1000, 3)

        ts = doc.get("timestamp") or datetime.now(timezone.utc).isoformat()
        return {
            "session_id":            sid,
            "event_time":            str(ts),
            "src_ip":                ip,
            "src_port":              int(doc.get("src_port")  or 0),
            "dst_port":              int(doc.get("dst_port")  or 22),
            "protocol":              protocol_raw,
            "duration":              float(doc.get("duration") or 0),
            "login_attempt_count":   int(fitur_values["login_attempt_count"]),
            "login_success_count":   int(fitur_values["login_success_count"]),
            "cmd_count":             int(fitur_values["cmd_count"]),
            "session_event_count":   int(fitur_values["session_event_count"]),
            "unique_username_count": int(fitur_values["unique_username_count"]),
            "unique_password_count": int(fitur_values["unique_password_count"]),
            "ip_total_events":       int(fitur_values["ip_total_events"]),
            "ip_total_sessions":     int(fitur_values["ip_total_sessions"]),
            "ip_total_login_fail":   int(fitur_values["ip_total_login_fail"]),
            "ip_total_login_ok":     int(fitur_values["ip_total_login_ok"]),
            "ip_total_downloads":    int(fitur_values["ip_total_downloads"]),
            "has_command":           int(fitur_values["has_command"]),
            "has_download":          int(fitur_values["has_download"]),
            "has_upload":            int(fitur_values["has_upload"]),
            "has_malware":           int(fitur_values["has_malware"]),
            "label_knn":             label_knn,
            "label_lr":              label_lr,
            "ground_truth":          ground_truth,
            "latency_ms":            latency,
            "latency_knn_ms":        latency_knn,
            "latency_lr_ms":         latency_lr,
        }

    # ----------------------------------------------------------
    def _simpan_postgresql(self, result: dict,
                            window_id: int,
                            window_start: datetime,
                            window_end: datetime):
        row = {**result,
               "window_id":    window_id,
               "window_start": window_start,
               "window_end":   window_end}
        try:
            self._pg_cur.execute(INSERT_SQL, row)
            inserted_id = self._pg_cur.fetchone()[0]
            if result["label_knn"] == "anomali" or result["label_lr"] == "anomali":
                self._pg_cur.execute(ALERT_SQL, {
                    "result_id": inserted_id,
                    "src_ip":    result["src_ip"],
                    "label_knn": result["label_knn"],
                    "label_lr":  result["label_lr"],
                })
        except Exception as e:
            log.error(f"  PostgreSQL error: {e}")
            try:
                self._pg_conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                    user=PG_USER, password=PG_PASSWORD, connect_timeout=10,
                )
                self._pg_conn.autocommit = True
                self._pg_cur = self._pg_conn.cursor()
                log.info("  [MapFn] reconnect PostgreSQL berhasil.")
            except Exception as re:
                log.error(f"  gagal reconnect PostgreSQL: {re}")

    # ----------------------------------------------------------
    def close(self):
        avg = round(self._total_latency / self._event_count, 3) \
              if self._event_count > 0 else 0
        if self._pg_cur:
            try: self._pg_cur.close()
            except: pass
        if self._pg_conn:
            try: self._pg_conn.close()
            except: pass
        log.info(
            f"  [MapFn] task selesai. "
            f"Total event: {self._event_count:,} | "
            f"Anomali kNN: {self._total_anomali_knn:,} | "
            f"Anomali LR: {self._total_anomali_lr:,} | "
            f"Avg latency: {avg:.3f}ms"
        )

# ============================================================
# JALANKAN SATU WINDOWING 
# ============================================================
def jalankan_flink_job(json_strings: list, fitur_names: list,
                        window_ctx: dict, label: str) -> tuple:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    json_strings_with_sentinel = json_strings + [KlasifikasiMapFunction.STATE_SENTINEL]

    ds = env.from_collection(
        collection = json_strings_with_sentinel,
        type_info  = Types.STRING()
    )

    window_ctx_json = json.dumps(window_ctx)
    map_fn    = KlasifikasiMapFunction(MODEL_DIR, fitur_names, window_ctx_json)
    result_ds = ds.map(map_fn, output_type=Types.STRING())

    log.info(f"  Submit job: {label} ...")
    results     = []
    state_akhir = None

    with result_ds.execute_and_collect(label) as results_iter:
        for r in results_iter:
            try:
                d = json.loads(r)
                if "__state_dump__" in d:
                    # ini adalah state akhir dari MapFunction
                    state_akhir = d["__state_dump__"]
                else:
                    results.append(r)
            except Exception:
                results.append(r)

    log.info(f"  job selesai. {len(results):,} hasil.")

    if state_akhir is None:
        log.warning("  [State] Sentinel tidak diterima, state tidak di-update.")
        state_akhir = window_ctx

    return results, state_akhir

def cetak_ringkasan(results: list, windowing: int):
    total       = len(results)
    anomali_knn = 0
    anomali_lr  = 0
    total_lat   = 0.0
    errors      = 0

    for r in results:
        try:
            d = json.loads(r)
            if "error" in d:
                errors += 1
                continue
            if d.get("label_knn") == "anomali": anomali_knn += 1
            if d.get("label_lr")  == "anomali": anomali_lr  += 1
            total_lat += d.get("latency_ms", 0)
        except Exception:
            errors += 1

    valid    = total - errors
    avg_lat  = round(total_lat / valid, 3) if valid > 0 else 0
    pct_knn  = round(anomali_knn / valid * 100, 2) if valid > 0 else 0
    pct_lr   = round(anomali_lr  / valid * 100, 2) if valid > 0 else 0

    log.info(f"\n{'='*65}")
    log.info(f"  📊 RINGKASAN WINDOWING #{windowing}")
    log.info(f"{'='*65}")
    log.info(f"  Total event diproses : {valid:,}")
    log.info(f"  Error / skip         : {errors:,}")
    log.info(f"  Anomali kNN          : {anomali_knn:,} ({pct_knn}%)")
    log.info(f"  Anomali LR           : {anomali_lr:,}  ({pct_lr}%)")
    log.info(f"  Avg latency          : {avg_lat:.3f} ms")
    log.info(f"{'='*65}\n")

# ============================================================
# MAIN
# ============================================================
def main():
    log.info("\n" + "=" * 65)
    log.info("STREAMING ANALYTICS — Submit ke Flink Cluster")
    log.info(f"  Environment  : {ENV_NAME}")
    log.info(f"  WINDOW_SIZE  : {WINDOW_SIZE}")
    log.info(f"  Model dir    : {MODEL_DIR}")
    log.info(f"  PostgreSQL   : {PG_HOST}:{PG_PORT}")
    log.info(f"  Flink UI     : http://localhost:8081")
    log.info("=" * 65)

    fitur_path = os.path.join(MODEL_DIR, "fitur_names.json")
    with open(fitur_path) as f:
        FITUR_NAMES = json.load(f)["fitur"]
    log.info(f"  {len(FITUR_NAMES)} fitur dimuat.")

    for mf in ["knn_model.pkl", "lr_model.pkl", "scaler.pkl",
               "label_encoder_protocol.pkl"]:
        if not os.path.exists(os.path.join(MODEL_DIR, mf)):
            log.error(f"Model tidak ditemukan: {mf}")
            sys.exit(1)

    try:
        c = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                             user=PG_USER, password=PG_PASSWORD, connect_timeout=5)
        c.close()
        log.info("  postgreSQL terhubung.")
    except Exception as e:
        log.error(f"gagal koneksi PostgreSQL: {e}")
        sys.exit(1)

    windowing = 0
    while True:
        windowing += 1
        log.info(f"\n{'─'*65}")
        log.info(f"  WINDOWING #{windowing} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info(f"{'─'*65}")

        hapus_state()

        try:
            total_docs = get_total_docs()
            log.info(f"  total dokumen MongoDB: {total_docs:,}")
        except Exception as e:
            log.error(f"  gagal koneksi MongoDB: {e}")
            time.sleep(10)
            continue

        window_ctx  = muat_state()
        all_results = []
        skip        = 0
        window_num   = 0

        while skip < total_docs:
            window_num += 1
            log.info(
                f"  windowing #{window_num} | "
                f"offset={skip:,} | "
                f"sisa={max(0, total_docs - skip):,}"
            )

            try:
                json_strings = fetch_mongo_window(skip, WINDOWING_SIZE)
            except Exception as e:
                log.error(f"  windowing #{window_num} gagal: {e}")
                time.sleep(5)
                continue

            if not json_strings:
                log.info(f"  windowing #{window_num} kosong, selesai.")
                break

            job_label = (
                f"Streaming Analytics — "
                f"windowing #{window_num} "
            )
            try:
                results, state_akhir = jalankan_flink_job(
                    json_strings, FITUR_NAMES, window_ctx, job_label
                )
                all_results.extend(results)

                window_ctx = state_akhir
                simpan_state(window_ctx)

                log.info(
                    f"  [State] state windowing #{window_num} disimpan: "
                    f"event_count={window_ctx.get('event_count',0):,}, "
                    f"window_id={window_ctx.get('window_id',1)}, "
                    f"sesi={len(window_ctx.get('sess_ctx',{})):,}, "
                    f"IP={len(window_ctx.get('ip_ctx',{})):,}"
                )

            except Exception as e:
                log.error(f"  Flink job windowing #{window_num} gagal: {e}")
                time.sleep(5)
                continue

            skip += len(json_strings)

        cetak_ringkasan(all_results, windowing)

        if REPEAT_DELAY_SECONDS > 0:
            log.info(f"  ⏳ Jeda {REPEAT_DELAY_SECONDS}s sebelum windowing #{windowing + 1}...")
            time.sleep(REPEAT_DELAY_SECONDS)

if __name__ == "__main__":
    main()