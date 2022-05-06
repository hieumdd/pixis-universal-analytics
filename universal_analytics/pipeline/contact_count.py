from datetime import datetime

from universal_analytics.pipeline.interface import Pipeline
from universal_analytics.repo import get_static, get_listing, static_params_fn

pipeline = Pipeline(
    "ContactCount",
    get_static(
        get_listing("call-campaign"),
        [{"active": 0}, {"active": 1}],
        "call-campaign/contact-count",
    ),
    lambda rows: [
        {
            "_id": row.get("_id"),
            "total": row.get("total"),
            "done": row.get("done"),
            "readyToCall": row.get("readyToCall"),
            "_batched_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        for row in rows
    ],
    [
        {"name": "_id", "type": "STRING"},
        {"name": "total", "type": "NUMERIC"},
        {"name": "done", "type": "NUMERIC"},
        {"name": "readyToCall", "type": "NUMERIC"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ],
    params_fn=static_params_fn,  # type: ignore
    cursor_key="_batched_at",
)
