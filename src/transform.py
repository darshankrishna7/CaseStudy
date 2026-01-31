import os
import json
import pandas as pd
import numpy as np

def clean_and_scale(rows: list[dict], run_id: str) -> dict:
    df = pd.DataFrame(rows)

    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

    df["f2"] = pd.to_numeric(df["f2"], errors="coerce")
    df["f2"] = df["f2"].fillna(df["f2"].median())

    for c in ["f1", "f2", "f3"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        mean = df[c].mean()
        std = df[c].std()
        if not std or pd.isna(std) or std == 0:
            std = 1.0
        df[f"{c}_z"] = (df[c] - mean) / std

    # Make data JSON-safe
    df["ts_utc"] = df["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    for c in ["f1", "f2", "f3", "f1_z", "f2_z", "f3_z"]:
        df[c] = df[c].astype(float)

    df = df.replace({np.nan: None})

    out_dir = "/opt/airflow/data"
    os.makedirs(out_dir, exist_ok=True)

    safe_run_id = run_id.replace(":", "_")
    out_path = os.path.join(out_dir, f"transformed_{safe_run_id}.json")

    with open(out_path, "w") as f:
        json.dump(df.to_dict(orient="records"), f)

    return {
        "path": out_path,
        "rows": len(df)
    }