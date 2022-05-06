from typing import Any, Callable, Optional
from dataclasses import dataclass, field

from universal_analytics.repo import update_params_fn

CREATE_TIME = "createTime"
UPDATE_TIME = "updateTime"


@dataclass
class Pipeline:
    name: str
    get: Callable[[Any], list[dict[str, Any]]]
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    schema: list[dict[str, Any]]
    cursor_key: str = UPDATE_TIME
    id_key: list[str] = field(default_factory=lambda: ["_id"])
    params_fn: Callable[[str, str], Optional[dict[str, Any]]] = update_params_fn()
