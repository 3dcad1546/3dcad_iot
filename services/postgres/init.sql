-- init.sql

-- 1) Key-Value Config Store
CREATE TABLE IF NOT EXISTS config (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Seed some default config values (won’t overwrite if already present)
INSERT INTO config (key, value) VALUES
  ('MACHINE_ID',              'MACHINE_XYZ'),
  ('CBS_STREAM_NAME',         'line1'),
  ('OPERATOR_IDS',            'op1,op2,op3'),
  ('ADMIN_IDS',               'admin1,admin2'),
  ('ENGINEER_IDS',            'eng1,eng2'),
  ('MES_OPERATOR_LOGIN_URL',  'http://mes-server/api/login'),
  ('MES_PROCESS_CONTROL_URL', 'http://mes-server/api/pc'),
  ('MES_UPLOAD_URL',          'http://mes-server/api/upload'),
  ('TRACE_PROXY_HOST',        'trace-proxy:8765'),
  ('FAILURE_REASON_CODES',    'F001,F002'),
  ('NCM_REASON_CODES',        'NCM1,NCM2'),
  ('INFLUXDB_URL',            'http://influxdb:8086'),
  ('INFLUXDB_TOKEN',          'edgetoken'),
  ('INFLUXDB_ORG',            'EdgeOrg'),
  ('INFLUXDB_BUCKET',         'EdgeBucket'),
  ('KAFKA_BROKER',            'kafka:9092')
ON CONFLICT (key) DO NOTHING;


-- 2) Machine-specific API URLs & enable flags
CREATE TABLE IF NOT EXISTS machine_config (
  id                        SERIAL   PRIMARY KEY,
  machine_id                TEXT     NOT NULL UNIQUE,
  mes_process_control_url   TEXT     NOT NULL,
  mes_upload_url            TEXT     NOT NULL,
  is_mes_enabled            BOOLEAN  NOT NULL DEFAULT TRUE,
  trace_process_control_url TEXT     NOT NULL
                               DEFAULT 'http://trace-proxy:8765/v2/process_control',
  trace_interlock_url       TEXT     NOT NULL
                               DEFAULT 'http://trace-proxy:8765/interlock',
  is_trace_enabled          BOOLEAN  NOT NULL DEFAULT TRUE
);

INSERT INTO machine_config (
    machine_id,
    mes_process_control_url,
    mes_upload_url,
    trace_process_control_url,
    trace_interlock_url,
    is_mes_enabled,
    is_trace_enabled
)
VALUES (
    'MyActualMachine123',                                      -- must match your MACHINE_ID
    'http://mes-server/api/pc',                                -- your MES PC URL
    'http://mes-server/api/upload',                            -- your MES Upload URL
    'http://trace-proxy:8765/v2/process_control',              -- Trace PC URL
    'http://trace-proxy:8765/interlock',                       -- Trace Interlock URL
    TRUE,                                                      -- enable MES calls
    TRUE                                                       -- enable Trace calls
)
ON CONFLICT (machine_id) DO NOTHING;
-- 3) Shift master + seeds
CREATE TABLE IF NOT EXISTS shift_master (
  id          SERIAL   PRIMARY KEY,
  name        TEXT     NOT NULL UNIQUE,
  start_time  TIME     NOT NULL,
  end_time    TIME     NOT NULL
);

INSERT INTO shift_master (name, start_time, end_time) VALUES
  ('Shift A', '06:00:00', '14:00:00'),
  ('Shift B', '14:00:00', '22:00:00'),
  ('Shift C', '22:00:00', '06:00:00')
ON CONFLICT (name) DO NOTHING;


-- 4) Token-based sessions for all roles
CREATE TYPE user_role AS ENUM ('operator','admin','engineer');

CREATE TABLE IF NOT EXISTS sessions (
  token      UUID       PRIMARY KEY,
  username   TEXT       NOT NULL,
  role       user_role  NOT NULL,
  shift_id   INTEGER    REFERENCES shift_master(id),
  login_ts   TIMESTAMP  NOT NULL DEFAULT NOW(),
  logout_ts  TIMESTAMP
);


-- 5) Audit of operator login/logout
CREATE TABLE IF NOT EXISTS operator_sessions (
  id         SERIAL     PRIMARY KEY,
  username   TEXT       NOT NULL,
  shift_id   INTEGER    REFERENCES shift_master(id),
  login_ts   TIMESTAMP  NOT NULL DEFAULT NOW(),
  logout_ts  TIMESTAMP
);


-- 6) Preserve PLC auto-mode bit at shift end
CREATE TABLE IF NOT EXISTS auto_status_log (
  id         SERIAL     PRIMARY KEY,
  shift_id   INTEGER    NOT NULL REFERENCES shift_master(id),
  status_val INTEGER,
  ts         TIMESTAMP  NOT NULL DEFAULT NOW()
);


-- 7) MES/Trace history
CREATE TABLE IF NOT EXISTS mes_trace_history (
  id            SERIAL   PRIMARY KEY,
  serial        TEXT     NOT NULL,
  step          TEXT     NOT NULL,       -- e.g. mes_pc, trace_pc, interlock…
  response_json JSONB    NOT NULL,
  ts            TIMESTAMP NOT NULL DEFAULT NOW()
);


-- 8) Scan audit trail
CREATE TABLE IF NOT EXISTS scan_audit (
  id        SERIAL   PRIMARY KEY,
  serial    TEXT     NOT NULL,
  operator  TEXT     NOT NULL,
  result    TEXT     NOT NULL,           -- pass|fail|scrap
  ts        TIMESTAMP NOT NULL DEFAULT NOW()
);


-- 9) Generic error logs
CREATE TABLE IF NOT EXISTS error_logs (
  id        SERIAL   PRIMARY KEY,
  context   TEXT     NOT NULL,           -- e.g. “scan”, “login”, etc.
  error_msg TEXT     NOT NULL,
  details   JSONB,
  ts        TIMESTAMP NOT NULL DEFAULT NOW()
);
