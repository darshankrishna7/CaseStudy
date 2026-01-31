from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context

from src.extract import fetch_batch
from src.transform import clean_and_scale
from src.load import upsert_processed, upsert_predictions
from src.ml import predict_batch

DEFAULT_ARGS = {
    "owner": "darshan",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="10_etl_ml_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["case-study", "etl", "ml"],
) as dag:

    @task
    def extract():
        try:
            rows = fetch_batch()
            logging.info("Extracted %d rows", len(rows))
            return rows
        except Exception as e:
            raise AirflowFailException(str(e))

    @task
    def transform(rows):
        try:
            context = get_current_context()
            run_id = context["run_id"]
            out = clean_and_scale(rows, run_id=run_id)
            logging.info("Transformed %d rows (written to %s)", out["rows"], out["path"])
            return out
        except Exception as e:
            raise AirflowFailException(str(e))

    @task
    def load_processed(meta):
        import json
        with open(meta["path"]) as f:
            rows = json.load(f)
        return upsert_processed(rows)

    @task
    def predict(meta):
        import json
        with open(meta["path"]) as f:
            rows = json.load(f)
        return predict_batch(rows, model_version="baseline_v1")

    @task
    def store_predictions(preds):
        return upsert_predictions(preds)

    data = extract()
    processed = transform(data)
    load_processed(processed)
    preds = predict(processed)
    store_predictions(preds)