from typing import Any, Callable, Union, Optional
import os
import asyncio
from datetime import date
from urllib.parse import urljoin

import httpx
from google.auth import default
from googleapiclient.discovery import build, Resource

VIEW_ID = 123
PAGE_SIZE = 50_000


def get_resource() -> Resource:
    credentials, _ = default()
    return build("analyticsreporting", "v4", credentials=credentials)


Builder = Callable[[Optional[str]], dict[str, Any]]


def build_request(
    dimensions: list[str],
    metrics: list[str],
    start: date,
    end: date,
) -> Builder:
    def _build(token: Optional[str]) -> dict[str, Any]:
        return {
            "dateRanges": {
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
            },
            "viewId": VIEW_ID,
            "dimensions": [{"name": f"ga:{dimension}"} for dimension in dimensions],
            "metrics": [{"expression": f"ga:{metric}"} for metric in metrics],
            "pageSize": PAGE_SIZE,
            **({"pageToken": token} if token else {}),
        }

    return _build

ColumnHeader = dict[str, Any]
Rows = dict[str, Any]
ReportRes = tuple[list[dict[str, Any]], list[dict[str, Any]]]


def get_report(
    resource: Resource,
    builders: list[Builder],
    tokens: list[Optional[str]],
    is_lasts: list[bool],
) -> list[ReportRes]:
    res = (
        resource.reports()
        .batchGet(
            body={
                "reportRequests": [
                    builder(token) for builder, token in zip(builders, tokens)
                ]
            }
        )
        .execute()["reports"]
    )
    column_headers = [i for i in res["columnHeader"]]
    tokens = [i for i in res["nextPageToken"]]
    rows = [
        row if is_last else [] for row, is_last in zip(res["data"]["rows"], is_lasts)
    ]
    is_lasts = [not bool(i) for i in tokens]

    _return = [(column_headers, rows)]

    return (
        _return
        if all(is_lasts)
        else _return + get_report(resource, builders, tokens, is_lasts)
    )
