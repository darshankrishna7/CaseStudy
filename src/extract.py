import random
import uuid
from datetime import datetime, timezone

def fetch_batch(batch_size: int = 200) -> list[dict]:
    now = datetime.now(timezone.utc)

    rows = []
    for _ in range(batch_size):
        rows.append({
            "event_id": str(uuid.uuid4()),
            "ts_utc": now.isoformat(),
            "f1": random.gauss(10, 2),
            "f2": None if random.random() < 0.05 else random.gauss(0, 1),
            "f3": random.gauss(100, 10),
        })

    return rows