def predict_batch(rows: list[dict], model_version: str) -> list[dict]:
    preds = []
    for r in rows:
        y_hat = (
            0.3 * r["f1_z"]
            + 0.2 * r["f2_z"]
            + 0.5 * r["f3_z"]
        )

        preds.append({
            "event_id": r["event_id"],
            "ts_utc": r["ts_utc"],
            "y_hat": float(y_hat),
            "model_version": model_version,
        })

    return preds