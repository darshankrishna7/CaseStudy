from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

import psycopg2
import os

DEFAULT_ARGS = {"owner": "darshan", "retries": 2, "retry_delay": timedelta(minutes=1)}

def _conn():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )

with DAG(
    dag_id="00_init_schema",
    start_date=datetime(2026, 1, 1),
    schedule=None,   # manual
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["case-study"],
) as dag:

    @task
    def init_schema() -> str:
        try:
            schema_path = Path("/opt/airflow/sql/schema.sql")
            sql = schema_path.read_text(encoding="utf-8")

            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)

            logging.info("Schema applied successfully.")
            return "ok"
        except Exception as e:
            logging.exception("Failed to apply schema")
            raise AirflowFailException(str(e))

    @task
    def sanity_check(_: str) -> None:
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM processed_data;")
                    n1 = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM predictions;")
                    n2 = cur.fetchone()[0]

            logging.info("Sanity check ok. processed_data=%s predictions=%s", n1, n2)
        except Exception as e:
            logging.exception("Sanity check failed")
            raise AirflowFailException(str(e))

    sanity_check(init_schema())