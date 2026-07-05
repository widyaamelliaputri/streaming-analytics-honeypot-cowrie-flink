-- ============================================================
-- File: docker/init.sql
-- Dijalankan otomatis saat container PostgreSQL pertama kali dibuat
-- ============================================================

-- ============================================================
-- INISIALISASI DATABASE POSTGRESQL UNTUK HASIL KLASIFIKASI
-- ============================================================

-- ============================================
-- INISIALISASI DATABASE POSTGRESQL
-- ============================================

-- Tabel untuk hasil klasifikasi
CREATE TABLE IF NOT EXISTS classification_result (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100),
    event_time TIMESTAMP WITH TIME ZONE,
    src_ip VARCHAR(50),
    src_port INTEGER,
    dst_port INTEGER,
    protocol VARCHAR(20),
    duration DOUBLE PRECISION,
    login_attempt_count INTEGER,
    login_success_count INTEGER,
    cmd_count INTEGER,
    session_event_count INTEGER,
    unique_username_count INTEGER,
    unique_password_count INTEGER,
    ip_total_events INTEGER,
    ip_total_sessions INTEGER,
    ip_total_login_fail INTEGER,
    ip_total_login_ok INTEGER,
    ip_total_downloads INTEGER,
    has_command INTEGER,
    has_download INTEGER,
    has_upload INTEGER,
    has_malware INTEGER,
    label_knn VARCHAR(20),
    label_lr VARCHAR(20),
    ground_truth INTEGER,
    latency_ms DOUBLE PRECISION,
    latency_knn_ms DOUBLE PRECISION,
    latency_lr_ms DOUBLE PRECISION,
    window_id INTEGER,
    window_start TIMESTAMP WITH TIME ZONE,
    window_end TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel untuk alert
CREATE TABLE IF NOT EXISTS alert_log (
    id SERIAL PRIMARY KEY,
    result_id INTEGER REFERENCES classification_result(id),
    src_ip VARCHAR(50),
    label_knn VARCHAR(20),
    label_lr VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index untuk query yang lebih cepat
CREATE INDEX IF NOT EXISTS idx_src_ip ON classification_result(src_ip);
CREATE INDEX IF NOT EXISTS idx_event_time ON classification_result(event_time);
CREATE INDEX IF NOT EXISTS idx_label_knn ON classification_result(label_knn);
CREATE INDEX IF NOT EXISTS idx_label_lr ON classification_result(label_lr);
CREATE INDEX IF NOT EXISTS idx_ground_truth ON classification_result(ground_truth);
CREATE INDEX IF NOT EXISTS idx_latency_knn ON classification_result(latency_knn_ms);
CREATE INDEX IF NOT EXISTS idx_latency_lr  ON classification_result(latency_lr_ms);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO skripsi;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO skripsi;