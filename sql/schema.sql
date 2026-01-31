CREATE TABLE IF NOT EXISTS processed_data (
  event_id        TEXT PRIMARY KEY,
  ts_utc          TIMESTAMPTZ NOT NULL,
  f1              DOUBLE PRECISION NOT NULL,
  f2              DOUBLE PRECISION NOT NULL,
  f3              DOUBLE PRECISION NOT NULL,
  f1_z            DOUBLE PRECISION NOT NULL,
  f2_z            DOUBLE PRECISION NOT NULL,
  f3_z            DOUBLE PRECISION NOT NULL,
  created_at_utc  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions (
  event_id          TEXT PRIMARY KEY,
  ts_utc            TIMESTAMPTZ NOT NULL,
  y_hat             DOUBLE PRECISION NOT NULL,
  model_version     TEXT NOT NULL,
  predicted_at_utc  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);