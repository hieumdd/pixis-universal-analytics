from typing import Any
from dataclasses import dataclass, field


@dataclass
class Report:
    name: str
    dimensions: list[str]
    metrics: list[str]
    schema: list[dict[str, Any]] = field(init=False)

    def __post_init__(self):
        self.schema = (
            [
                {"name": key, "type": "DATE" if key == "date" else "STRING"}
                for key in self.dimensions
            ]
            + [{"name": key, "type": "NUMERIC"} for key in self.metrics]
            + [
                {"name": "_batched_at", "type": "TIMESTAMP"}
            ]
        )
