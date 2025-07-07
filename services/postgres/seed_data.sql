-- 1) Seed Users (admin, engineer, operator)
INSERT INTO users (id, first_name, last_name, username, password_hash, role) VALUES
  ('00000000-0000-0000-0000-000000000001', 'Alice', 'Admin',    'admin1', '$2b$12$JLlZue0vhDrj/SsSSsSSSuuFrqw3KqXnb4GiUqD33dZbRNaVv7MEK', 'admin'),   -- password: admin123
  ('00000000-0000-0000-0000-000000000002', 'Eve',   'Engineer', 'eng1',   '$2b$12$YdYdYdYdYdYdYdYdYdYdYeEJKjwjDFkUljkkkQ33kDfdsDFdsls.lq', 'engineer'), -- password: eng123
  ('00000000-0000-0000-0000-000000000003', 'Oscar', 'Operator', 'op1',    '$2b$12$1H6lKJKJKJKJKJKJKJKJKJuYuOAo.ZB67t2CHAbZt3gXqpRuU7qxu6', 'operator')  -- password: op123
ON CONFLICT (username) DO NOTHING;

-- 2) Seed Access Rights
INSERT INTO user_access (role, page_name, can_read, can_write) VALUES
  ('admin',    'dashboard', true, true),
  ('admin',    'users', true, true),
  ('engineer', 'dashboard', true, true),
  ('operator', 'dashboard', true, false)
ON CONFLICT DO NOTHING;

-- 3) Seed Message Master
INSERT INTO message_master (code, message) VALUES
  ('E001', 'Invalid serial number'),
  ('E002', 'MES check failed'),
  ('I001', 'Part uploaded successfully')
ON CONFLICT (code) DO NOTHING;

-- 4) Seed PLC Tests
INSERT INTO plc_test (name, param1, param2, param3, param4, param5, param6) VALUES
  ('Test A', true, false, true, false, true, true),
  ('Test B', false, false, false, false, false, false);

-- 5) Seed Shift Master (if not already seeded)
INSERT INTO shift_master (name, start_time, end_time) VALUES
  ('Shift A', '06:00:00', '14:00:00'),
  ('Shift B', '14:00:00', '22:00:00'),
  ('Shift C', '22:00:00', '06:00:00')
ON CONFLICT (name) DO NOTHING;

-- 6) Seed Machine Config
INSERT INTO machine_config (
    machine_id,
    mes_process_control_url,
    mes_upload_url,
    trace_process_control_url,
    trace_interlock_url,
    is_mes_enabled,
    is_trace_enabled
) VALUES (
    'TestMachine1',
    'http://mes-server/api/pc',
    'http://mes-server/api/upload',
    'http://trace-proxy:8765/v2/process_control',
    'http://trace-proxy:8765/interlock',
    TRUE,
    TRUE
)
ON CONFLICT (machine_id) DO NOTHING;
