from psycopg2.extras import execute_values
from src.db import get_conn

def upsert_processed(rows: list[dict]) -> int:
    sql = """
    INSERT INTO processed_data
    (event_id, ts_utc, f1, f2, f3, f1_z, f2_z, f3_z)
    VALUES %s
    ON CONFLICT (event_id) DO UPDATE SET
      ts_utc = EXCLUDED.ts_utc,
      f1 = EXCLUDED.f1,
      f2 = EXCLUDED.f2,
      f3 = EXCLUDED.f3,
      f1_z = EXCLUDED.f1_z,
      f2_z = EXCLUDED.f2_z,
      f3_z = EXCLUDED.f3_z;
    """

    values = [
        (r["event_id"], r["ts_utc"], r["f1"], r["f2"], r["f3"],
         r["f1_z"], r["f2_z"], r["f3_z"])
        for r in rows
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)

    return len(rows)


def upsert_predictions(rows: list[dict]) -> int:
    sql = """
    INSERT INTO predictions
    (event_id, ts_utc, y_hat, model_version)
    VALUES %s
    ON CONFLICT (event_id) DO UPDATE SET
      y_hat = EXCLUDED.y_hat,
      model_version = EXCLUDED.model_version,
      predicted_at_utc = NOW();
    """

    values = [
        (r["event_id"], r["ts_utc"], r["y_hat"], r["model_version"])
        for r in rows
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)

    return len(rows)