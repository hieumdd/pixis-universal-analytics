from typing import Any

from ua.ua_controller import (
    ua_controller,
)


def main(request) -> dict[str, Any]:
    body: dict[str, Any] = request.get_json()

    print(body)

    result = ua_controller(body)

    print(result)

    return result
