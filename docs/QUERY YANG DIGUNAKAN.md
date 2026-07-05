docker exec -it postgres-hasil psql -U skripsi -d hasil_klasifikasi
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

hasil_klasifikasi=# SELECT * FROM classification_result WHERE label_knn = 'anomali' OR label_lr = 'anomali' ORDER BY created_at DESC LIMIT 5;            ^
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