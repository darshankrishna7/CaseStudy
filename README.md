# Airflow ETL + ML Pipeline Case Study

## Overview

This project implements an end-to-end ETL and machine learning inference pipeline using **Apache Airflow**. The pipeline runs every 10 minutes, processes incoming event data, performs feature engineering, runs a simple ML model, and stores predictions in PostgreSQL.

The solution focuses on:

- production-style Airflow DAG design
- idempotent data loading
- safe task-to-task communication
- clear separation of concerns

---

## Architecture

**Pipeline flow:**

```
extract → transform → load_processed → predict → store_predictions
```

- **Extract**: Simulates an upstream data source by generating a batch of event records.
- **Transform**: Cleans data, handles missing values, performs z-score normalization, and writes results to disk.
- **Load**: Inserts processed data into PostgreSQL using UPSERT semantics.
- **Predict**: Runs a deterministic baseline ML model.
- **Store Predictions**: Persists predictions with model versioning.

To avoid passing large datasets via XCom, intermediate data is written to disk and only lightweight metadata is passed between tasks.

---

## Tech Stack

- Apache Airflow 2.9
- PostgreSQL
- Docker & Docker Compose
- Python (pandas, psycopg2)

---

## How to Run

### Prerequisites

- Docker
- Docker Compose

### Start the pipeline

```bash
docker compose up airflow-init
docker compose up -d
```

Access Airflow UI:

```
http://localhost:8080
```

Login credentials:

- **Username**: admin
- **Password**: admin

Enable and trigger the following DAGs:

- `00_init_schema`
- `10_etl_ml_pipeline`

---

## Database Tables

### processed\_data

Stores cleaned and normalized features derived from the raw event data.

### predictions

Stores machine learning predictions along with model versioning and timestamps.

Both tables use primary keys and UPSERT logic to avoid duplicate records when DAGs are re-run.

---

## ML Model

A simple deterministic linear model is used for demonstration:

```
ŷ = 0.3·f1_z + 0.2·f2_z + 0.5·f3_z
```

The model is intentionally simple to keep the focus on pipeline orchestration, data flow, and reliability rather than model complexity.

---

## Production Considerations

- Large datasets should be exchanged via files or object storage rather than XCom
- External APIs or streaming systems can replace the mocked extract step
- Model versioning enables safe iteration and rollback
- The pipeline is idempotent and safe to rerun

---

## Assumptions & Trade-offs

- The data source is mocked to focus on pipeline design and orchestration
- Local filesystem storage is used instead of S3 for simplicity
- Batch size is fixed per pipeline run

---

## Author

Darshankrishna

