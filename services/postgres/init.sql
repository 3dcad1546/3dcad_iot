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
  trace_process_control_url TEXT     NOT NULL DEFAULT 'http://trace-proxy:8765/v2/process_control',
  trace_interlock_url       TEXT     NOT NULL DEFAULT 'http://trace-proxy:8765/interlock',
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
    'MyActualMachine123',
    'http://mes-server/api/pc',
    'http://mes-server/api/upload',
    'http://trace-proxy:8765/v2/process_control',
    'http://trace-proxy:8765/interlock',
    TRUE,
    TRUE
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
  ('Shift B', '14:00:00', '17:30:00'),
  ('Shift C', '17:30:00', '06:00:00')
ON CONFLICT (name) DO UPDATE SET
  start_time = EXCLUDED.start_time,
  end_time   = EXCLUDED.end_time;

-- 4) Token-based sessions for all roles
CREATE TYPE IF NOT EXISTS user_role AS ENUM ('operator','admin','engineer');

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
  step          TEXT     NOT NULL,
  response_json JSONB    NOT NULL,
  ts            TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 8) Scan audit trail
CREATE TABLE IF NOT EXISTS scan_audit (
  id        SERIAL   PRIMARY KEY,
  serial    TEXT     NOT NULL,
  operator  TEXT     NOT NULL,
  result    TEXT     NOT NULL,
  ts        TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 9) Generic error logs
CREATE TABLE IF NOT EXISTS error_logs (
  id        SERIAL   PRIMARY KEY,
  context   TEXT     NOT NULL,
  error_msg TEXT     NOT NULL,
  details   JSONB,
  ts        TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 10) Local users for admin & engineer
CREATE TABLE IF NOT EXISTS users (
  id           UUID PRIMARY KEY ,
  first_name   TEXT NOT NULL,
  last_name    TEXT NOT NULL,
  username     TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role         user_role NOT NULL
);

INSERT INTO users (username, password_hash, role) VALUES
  ('admin1', '$2b$12$KIX/OKIXlh1pGi1H/abc00abc1234567890abcdefghiJklmnopqr', 'admin'),
  ('eng1',   '$2b$12$7dT/7dTxyzXYZxyzXYZabc1234567890abcdefghiJklmnopqrs', 'engineer')
ON CONFLICT (username) DO NOTHING;

-- 11) User access table
CREATE TABLE IF NOT EXISTS user_access (
  role       user_role NOT NULL,
  page_name  TEXT      NOT NULL,
  can_read   BOOLEAN   NOT NULL DEFAULT TRUE,
  can_write  BOOLEAN   NOT NULL DEFAULT FALSE,
  PRIMARY KEY(role, page_name)
);

-- 12) Message master
CREATE TABLE IF NOT EXISTS message_master (
  code    TEXT PRIMARY KEY,
  message TEXT NOT NULL
);

-- 13) PLC test parameters
CREATE TABLE IF NOT EXISTS plc_test (
  id      SERIAL PRIMARY KEY,
  name    TEXT NOT NULL,
  param1  BOOLEAN NOT NULL,
  param2  BOOLEAN NOT NULL,
  param3  BOOLEAN NOT NULL,
  param4  BOOLEAN NOT NULL,
  param5  BOOLEAN NOT NULL,
  param6  BOOLEAN NOT NULL
);

-- 14) Cycle master + per-stage events
CREATE TABLE IF NOT EXISTS cycle_master (
  cycle_id      TEXT        PRIMARY KEY,
  operator      TEXT        NOT NULL,
  variant       TEXT        NOT NULL,
  barcode       TEXT        NOT NULL,
  shift_id      INTEGER     NOT NULL REFERENCES shift_master(id),
  start_ts      TIMESTAMP   NOT NULL DEFAULT NOW(),
  end_ts        TIMESTAMP,
  unload_status BOOLEAN     NOT NULL DEFAULT FALSE,
  unload_ts     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cycle_event (
  id          SERIAL      PRIMARY KEY,
  cycle_id    TEXT        NOT NULL REFERENCES cycle_master(cycle_id),
  stage       TEXT        NOT NULL,
  ts          TIMESTAMP   NOT NULL DEFAULT NOW()
);
--15)Vision Analytics Data
CREATE TABLE IF NOT EXISTS cycle_analytics (
    id SERIAL PRIMARY KEY,
    cycle_id TEXT REFERENCES cycle_master(cycle_id),
    operator TEXT NOT NULL,
    variant       TEXT        NOT NULL,
    barcode       TEXT        ,
    shift_id      INTEGER     NOT NULL REFERENCES shift_master(id),
    json_data JSONB NOT NULL,
    received_ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cycle_analytics_cycle_id ON cycle_analytics(cycle_id);

--16) Alarm master table 
CREATE TABLE IF NOT EXISTS alarm_master (
    id SERIAL PRIMARY KEY,
    alarm_date DATE NOT NULL,
    alarm_time TIME NOT NULL,
    alarm_code TEXT NOT NULL,
    message TEXT,
    status TEXT DEFAULT 'active',
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP,
    CONSTRAINT check_alarm_status CHECK (status IN ('active', 'acknowledged', 'resolved'))
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_alarm_master_date_time 
ON alarm_master(alarm_date DESC, alarm_time DESC);

CREATE INDEX IF NOT EXISTS idx_alarm_master_status 
ON alarm_master(status);

