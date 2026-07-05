## TAHAP 0 — Persiapan Awal (Lakukan Sekali)

### 1. Install Software yang Dibutuhkan

| Software          | Fungsi                          |
|-------------------|---------------------------------|
| Python 3.11.9       | Runtime utama                 |
| Visual Studio Code| Editor kode                     | 
| Docker Desktop    | Menjalankan Flink & PostgreSQL  | 
| MongoDB Compass   | Melihat data honeypot           | 

lalu cek untuk mengetahui mongodb yang digunakan adalah standalone instance atau replica set
Buka terminal di VS Code, lalu:
mongosh --eval "rs.status()"

Kalau hasilnya seperti ini → standalone:
MongoServerError: not running with --replSet
atau
{ "ok" : 0, "errmsg" : "not running with --replSet" }

Kalau hasilnya seperti ini → replica set:
json{
  "set" : "rs0",
  "members" : [ ... ],
  "ok" : 1
}

### 2. Update pip dan setuptools
```bash
python.exe -m pip install --upgrade pip
pip install --upgrade setuptools
```

### 3. Setup Virtual Environment (di CMD)
```bash
# Arahkan ke folder skripsi
cd C:\skripsi

# Buat dan aktifkan virtual environment
python -m venv venv
venv\Scripts\activate
# Tanda virtual environment aktif: muncul `(venv)` di awal baris terminal

# Install Apache Flink
pip install apache-flink==1.17.2

# Install semua library yang dibutuhkan
pip install -r ../requirements.txt
```

## URUTAN MENJALANKAN KODE PROGRAM
### TAHAP 1 — EDA (Eksplorasi Data)
Running file 01_eda.ipynb atau buka terminal dalan jalankan kode: 
```bash
cd notebooks
python notebooks/01_eda.ipynb
```

### TAHAP 2 — Preprocessing & Feature Engineering
Running file 02_preprocessing.ipynb atau buka terminal dalan jalankan kode:
```bash
python notebooks/02_preprocessing.ipynb
```

### TAHAP 3 — Training Model
Running file 03_model_training.ipynb atau buka terminal dalan jalankan kode:
```bash
python notebooks/03_model_training.ipynb
```

### TAHAP 4 — Jalankan Docker (Flink + PostgreSQL)
Buka Docker Desktop → tunggu sampai statusnya Running. Lalu di terminal VS Code:
```bash
cd docker
docker-compose up -d
# Cek apakah berhasil:
docker ps
# Harus muncul 3container yang statusnya `Up`: `flink-jobmanager`, `flink-taskmanager`, `postgres-hasil`.
# Verifikasi Flink berjalan → buka browser: **http://localhost:8081**
```

### TAHAP 5 — Jalankan Pipeline Streaming
Langsung running dari file streaming_job.py
Bisa juga melihat sudah berapa data yang tersimpan ke PostgreSQL, caranya dengan Buka terminal baru (biarkan streaming tetap jalan), lalu ketik kode berikut:
```bash
docker exec -it postgres-hasil psql -U skripsi -d hasil_klasifikasi -c "SELECT COUNT(*) FROM classification_result;"
atau
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U skripsi -h 127.0.0.1 -p 5432 -d hasil_klasifikasi -c "SELECT COUNT(*) FROM classification_result;"
```

### TAHAP 6 — Evaluasi Hasil
Langsung running dari file 04_evaluasi.py atau buka terminal dalan jalankan kode:
```bash
python notebooks/04_evaluasi.py
& C:/skripsi/venv/Scripts/python.exe c:/skripsi/notebooks/04_evaluasi.py
```

## Langkah-langkah Submit PyFlink Job
### Step 1: Copy file Python ke dalam container
Dari terminal VS Code (di luar container) di folder C:\skripsi, jalankan:
```bash
# STEP 1: Buat folder di container
docker exec flink-jobmanager mkdir -p /opt/data/models
docker exec flink-jobmanager mkdir -p /opt/data/processed

# STEP 2: Copy script
docker cp src/processing/streaming_job.py        flink-jobmanager:/opt/flink/usrlib/streaming_job.py
docker cp notebooks/04_evaluasi.py               flink-jobmanager:/opt/flink/usrlib/04_evaluasi.py
docker cp requirements.txt                       flink-jobmanager:/opt/flink/usrlib/requirements.txt

# STEP 3: Copy semua file model
docker cp data/models/fitur_names.json            flink-jobmanager:/opt/data/models/
docker cp data/models/knn_model.pkl               flink-jobmanager:/opt/data/models/
docker cp data/models/lr_model.pkl                flink-jobmanager:/opt/data/models/
docker cp data/models/scaler.pkl                  flink-jobmanager:/opt/data/models/
docker cp data/models/label_encoder_protocol.pkl  flink-jobmanager:/opt/data/models/
docker cp data/models/metrics_awal.json           flink-jobmanager:/opt/data/models/
```
Jika ada file yang salah di copy, hapus file dengan kode berikut:
```bash
docker exec -it flink-jobmanager bash
rm /opt/flink/usrlib/streaming_job.py
exit
```
Gunakan perintah `rm -rf` untuk menghapus semua isi folder sekaligus:
```bash
# Hapus semua isi folder (folder-nya tetap ada)
# Hati-hati dengan `rm -rf`, karena perintahnya tidak bisa di-undo dan tidak ada konfirmasi.
rm -rf /opt/data/models/*
rm -rf /opt/flink/usrlib/*
# Hapus folder beserta seluruh isinya (folder ikut terhapus)
rm -rf /opt/data/models
rm -rf /opt/flink/usrlib
# Verifikasi sudah kosong
ls /opt/flink/usrlib/
ls /opt/data/models/
```

### Step 2: Masuk ke dalam container Flink JobManager
```bash
docker exec -it flink-jobmanager bash

# Lihat isi direktori saat ini
ls
# Lihat lebih detail (ukuran, permission, tanggal)
ls -lh
# Lihat termasuk file tersembunyi (yang diawali titik)
ls -lha
# Lihat isi direktori tertentu (verifikasi semua file yang dicopy sudah masuk)
ls -lh /opt/flink/usrlib/
ls -lh /opt/data/models/
ls -lh /opt/data/processed/
```

### Step 3: Cek versi Python di dalam container
```bash
python --version
# Harus Python 3.9+
# Jika Python belum tersedia, install dulu:
apt-get update && apt-get install -y python3 python3-pip
ln -s /usr/bin/python3 /usr/bin/python
```

### Step 4: Install dependency PyFlink dan library yang dibutuhkan
```bash
pip install apache-flink==1.17.2
pip install -r /opt/flink/usrlib/requirements.txt
pip install "numpy<2" --force-reinstall
```

### Step 5: Submit PyFlink job via CLI
```bash
./bin/flink run -py /opt/flink/usrlib/streaming_job.py
```

### Step 6: Verifikasi di Flink Web UI
Buka browser → `http://localhost:8081` → tab Running Jobs

## Langkah-langkah Running 04_evaluasi.py
```bash
./bin/flink run -py /opt/flink/usrlib/04_evaluasi.py

exit
docker exec -it --user root flink-jobmanager bash
chmod -R 777 /opt/data/processed 
chmod -R 777 /opt/data/processed/windowing_state.json
exit
docker exec -it flink-jobmanager bash
./bin/flink run -py /opt/flink/usrlib/04_evaluasi.py
```

## Query untuk menghapus semua isi tabel tanpa menghapus struktur tabelnya:
```bash
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U skripsi -h 127.0.0.1 -p 5432 -d hasil_klasifikasi -c "TRUNCATE TABLE classification_result RESTART IDENTITY CASCADE;"
```
Penjelasan tiap bagian:
TRUNCATE TABLE classification_result → hapus semua baris data di tabel
RESTART IDENTITY → reset auto-increment ID kembali ke 1
CASCADE → otomatis hapus juga data di tabel alert_log yang berelasi (karena alert_log punya foreign key ke classification_result)

Cara menjalankannya di terminal Docker:
```
docker exec -it postgres-hasil psql -U skripsi -d hasil_klasifikasi -c "TRUNCATE TABLE classification_result RESTART IDENTITY CASCADE;"
```

## Cara masuk ke database hasil_klasifikasi di dalam container Docker.
docker exec -it postgres-hasil psql -U skripsi -d hasil_klasifikasi

cek jumlah data di PostgreSQL
SELECT COUNT(*) FROM classification count;
SELECT COUNT(*) FROM alert_log;

SELECT * FROM classification_result WHERE knn_label = 'anomali' OR lr_label = 'anomali' ORDER BY timestamp DESC LIMIT 20;


### Recreate container dari awal supaya init.sql dijalankan ulang dengan schema yang benar.
```bash
docker compose -f C:\skripsi\docker\docker-compose.yaml down -v
docker-compose up -d
```


Query yang digunakan untuk analisis:
(venv) PS C:\skripsi\docker> docker exec -it postgres-hasil psql -U skripsi -d hasil_klasifikasi
psql (13.23 (Debian 13.23-1.pgdg13+1))
Type "help" for help.

hasil_klasifikasi=# \dt
                List of relations
 Schema |         Name          | Type  |  Owner  
--------+-----------------------+-------+---------
 public | alert_log             | table | skripsi
 public | classification_result | table | skripsi
(2 rows)

hasil_klasifikasi=# \d classification_result
hasil_klasifikasi=# \d alert_log
                                        Table "public.alert_log"
   Column   |            Type             | Collation | Nullable |                Default                
------------+-----------------------------+-----------+----------+---------------------------------------
 id         | integer                     |           | not null | nextval('alert_log_id_seq'::regclass)
 result_id  | integer                     |           |          | 
 src_ip     | character varying(50)       |           |          | 
 label_knn  | character varying(20)       |           |          | 
 label_lr   | character varying(20)       |           |          | 
 created_at | timestamp without time zone |           |          | CURRENT_TIMESTAMP
Indexes:
    "alert_log_pkey" PRIMARY KEY, btree (id)
Foreign-key constraints:
    "alert_log_result_id_fkey" FOREIGN KEY (result_id) REFERENCES classification_result(id)

hasil_klasifikasi=# SELECT * FROM classification_resul LIMIT 5;
ERROR:  relation "classification_resul" does not exist
LINE 1: SELECT * FROM classification_resul LIMIT 5;
                      ^
hasil_klasifikasi=# SELECT * FROM classification_result LIMIT 5;
hasil_klasifikasi=# SELECT * FROM classification_result ORDER BY created_at DESC LIMIT 5;
hasil_klasifikasi=# SELECT COUNT(*) FROM classification_result;
 count 
-------
 20000
(1 row)

hasil_klasifikasi=# SELECT COUNT(*) FROM alert_log;
 count 
-------
 13886
(1 row)

hasil_klasifikasi=# SELECT id, src_ip, protocol, label_knn, label_lr, latency_ms, created_at FROM classification_result ORDER BY created_at DESC LIMIT 25;
  id   |        src_ip         | protocol | label_knn | label_lr | latency_ms |         created_at         
-------+-----------------------+----------+-----------+----------+------------+----------------------------
 20000 | ::ffff:78.189.117.110 | unknown  | normal    | normal   |     13.342 | 2026-06-25 03:58:40.371319
 19999 | 175.199.116.161       | unknown  | normal    | normal   |     15.004 | 2026-06-25 03:58:40.353194
 19998 | 175.199.116.161       | unknown  | normal    | anomali  |     13.788 | 2026-06-25 03:58:40.328871
 19997 | 175.199.116.161       | unknown  | normal    | anomali  |      11.35 | 2026-06-25 03:58:40.307236
 19996 | 175.199.116.161       | ssh      | normal    | normal   |     13.021 | 2026-06-25 03:58:40.287976
 19995 | ::ffff:78.189.117.110 | telnet   | normal    | normal   |     12.068 | 2026-06-25 03:58:40.270562
 19994 | ::ffff:163.120.103.93 | unknown  | normal    | normal   |     10.962 | 2026-06-25 03:58:40.25424
 19993 | ::ffff:163.120.103.93 | telnet   | normal    | normal   |     12.067 | 2026-06-25 03:58:40.23848
 19992 | 188.166.225.133       | unknown  | normal    | normal   |     14.699 | 2026-06-25 03:58:40.221547
 19991 | 188.166.225.133       | unknown  | anomali   | anomali  |     12.535 | 2026-06-25 03:58:40.199821
 19990 | 188.166.225.133       | unknown  | normal    | anomali  |     15.538 | 2026-06-25 03:58:40.180056
 19989 | 188.166.225.133       | unknown  | normal    | anomali  |     13.752 | 2026-06-25 03:58:40.155543
 19988 | 188.166.225.133       | ssh      | normal    | normal   |     10.917 | 2026-06-25 03:58:40.137373
 19987 | 181.215.176.227       | unknown  | anomali   | normal   |     11.533 | 2026-06-25 03:58:40.118438
 19986 | 181.215.176.227       | unknown  | anomali   | anomali  |     11.922 | 2026-06-25 03:58:40.099893
 19985 | 181.215.176.227       | unknown  | anomali   | anomali  |     13.222 | 2026-06-25 03:58:40.079413
 19984 | 181.215.176.227       | unknown  | anomali   | anomali  |     11.981 | 2026-06-25 03:58:40.060034
 19983 | 181.215.176.227       | unknown  | normal    | anomali  |     12.276 | 2026-06-25 03:58:40.038771
 19982 | 181.215.176.227       | unknown  | normal    | anomali  |     12.772 | 2026-06-25 03:58:40.006268
 19981 | 181.215.176.227       | ssh      | normal    | normal   |     13.138 | 2026-06-25 03:58:39.988586
 19980 | 222.120.5.98          | unknown  | normal    | normal   |     13.006 | 2026-06-25 03:58:39.97095
 19979 | 222.120.5.98          | unknown  | normal    | normal   |     12.409 | 2026-06-25 03:58:39.953369
 19978 | 222.120.5.98          | unknown  | anomali   | anomali  |     12.712 | 2026-06-25 03:58:39.934699
 19977 | 222.120.5.98          | unknown  | anomali   | anomali  |     12.225 | 2026-06-25 03:58:39.915409
 19976 | 222.120.5.98          | unknown  | anomali   | anomali  |      13.86 | 2026-06-25 03:58:39.896423
(25 rows)

hasil_klasifikasi=# SELECT * FROM classification_result WHERE knn_label = 'anomali' OR lr_label = 'anomali' ORDER BY timestamp DESC LIMIT 5;
ERROR:  column "knn_label" does not exist
LINE 1: SELECT * FROM classification_result WHERE knn_label = 'anoma...
                                                  ^
hasil_klasifikasi=# \d classification_result
hasil_klasifikasi=# SELECT * FROM classification_result WHERE label_knn = 'anomali' OR label_lr = 'anomali' ORDER BY timestamp DESC LIMIT 5;
ERROR:  column "timestamp" does not exist
LINE 1: ..._knn = 'anomali' OR label_lr = 'anomali' ORDER BY timestamp ...
                                                             ^
hasil_klasifikasi=# \d classification_result
hasil_klasifikasi=# SELECT * FROM classification_result WHERE label_knn = 'anomali' OR label_lr = 'anomali' ORDER BY created_at DESC LIMIT 5;
hasil_klasifikasi=# SELECT knn_label, COUNT(*) AS jumlah FROM classification_result GROUP BY knn_label;
ERROR:  column "knn_label" does not exist
LINE 1: SELECT knn_label, COUNT(*) AS jumlah FROM classification_res...
               ^
hasil_klasifikasi=# SELECT label_knn, COUNT(*) AS jumlah FROM classification_result GROUP BY label_knn;
 label_knn | jumlah 
-----------+--------
 anomali   |  10739
 normal    |   9261
(2 rows)

hasil_klasifikasi=# SELECT label_lr, COUNT(*) AS jumlah FROM classification_result GROUP BY label_lr;
 label_lr | jumlah 
----------+--------
 anomali  |  13445
 normal   |   6555
(2 rows)

hasil_klasifikasi=# 