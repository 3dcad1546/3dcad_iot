-- Configuration key-value store
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Track operator login sessions
CREATE TABLE IF NOT EXISTS current_operator (
  machine_id   TEXT PRIMARY KEY,
  username     TEXT NOT NULL,
  login_ts     TIMESTAMP NOT NULL DEFAULT NOW(),
  logout_ts    TIMESTAMP
);


-- Log every step of MES/Trace processing
CREATE TABLE IF NOT EXISTS mes_trace_history (
    id SERIAL PRIMARY KEY,
    serial TEXT NOT NULL,
    step TEXT NOT NULL,  -- e.g., mes_pc, trace_pc, interlock, etc.
    response_json JSONB NOT NULL,
    ts TIMESTAMP DEFAULT NOW()
);

-- Final scan audit trail
CREATE TABLE IF NOT EXISTS scan_audit (
    id SERIAL PRIMARY KEY,
    serial TEXT NOT NULL,
    operator TEXT NOT NULL,
    result TEXT NOT NULL,  -- "pass", "fail", "scrap"
    ts TIMESTAMP DEFAULT NOW()
);

-- Generic error logs for troubleshooting
CREATE TABLE IF NOT EXISTS error_logs (
    id SERIAL PRIMARY KEY,
    context TEXT NOT NULL,          -- e.g., "scan", "login", etc.
    error_msg TEXT NOT NULL,
    details JSONB,
    ts TIMESTAMP DEFAULT NOW()
);

-- Initial config values (optional insert)
INSERT INTO config (key, value) VALUES
('MACHINE_ID', 'MACHINE_XYZ'),
('CBS_STREAM_NAME', 'line1'),
('OPERATOR_IDS', 'op1,op2,op3'),
('MES_OPERATOR_LOGIN_URL', 'http://mes-server/api/login'),
('MES_PROCESS_CONTROL_URL', 'http://mes-server/api/pc'),
('MES_UPLOAD_URL', 'http://mes-server/api/upload'),
('TRACE_PROXY_HOST', 'trace-proxy'),
('FAILURE_REASON_CODES', 'F001,F002'),
('NCM_REASON_CODES', 'NCM1,NCM2'),
('INFLUXDB_URL', 'http://influxdb:8086'),
('INFLUXDB_TOKEN', 'edgetoken'),
('INFLUXDB_ORG', 'EdgeOrg'),
('INFLUXDB_BUCKET', 'EdgeBucket'),
('KAFKA_BROKER', 'kafka:9092')
ON CONFLICT (key) DO NOTHING;



-- ── 1) Shift master ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS shift_master (
    id          SERIAL      PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    start_time  TIME        NOT NULL,
    end_time    TIME        NOT NULL
);

-- Seed your three shifts (only inserts if name is absent)
INSERT INTO shift_master (name, start_time, end_time) VALUES
  ('Shift A', '06:00:00', '14:00:00'),
  ('Shift B', '14:00:00', '22:00:00'),
  ('Shift C', '22:00:00', '06:00:00')
ON CONFLICT (name) DO NOTHING;

-- ── 2) Machine config ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS machine_config (
    id                       SERIAL      PRIMARY KEY,
    machine_id               TEXT        NOT NULL UNIQUE,
    mes_process_control_url  TEXT        NOT NULL,
    mes_upload_url           TEXT        NOT NULL,
    is_mes_enabled           BOOLEAN     NOT NULL DEFAULT TRUE
);

-- (no seed—fill per–machine in your dashboard or via psql)
