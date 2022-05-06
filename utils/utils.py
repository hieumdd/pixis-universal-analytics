from typing import Optional
from datetime import datetime, timezone


def parse_unix_ts(x: Optional[int]) -> Optional[str]:
    try:
        if x == 0:
            return None
        return (
            datetime.fromtimestamp(x / 1e3)
            .astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            if x
            else None
        )
    except:
        return None
