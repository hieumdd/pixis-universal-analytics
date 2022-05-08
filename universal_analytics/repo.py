from typing import Any, Callable, Optional
from datetime import date, datetime

from google.auth import default
from googleapiclient.discovery import build, Resource

VIEW_ID = "247980254"
# PAGE_SIZE = 50_000
PAGE_SIZE = 10


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
Row = dict[str, Any]
ReportRes = tuple[list[ColumnHeader], list[Row]]


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
    column_headers = [i["columnHeader"] for i in res]
    tokens = [i.get("nextPageToken") for i in res]
    rows = [
        row["data"]["rows"] if not is_last else []
        for row, is_last in zip(res, is_lasts)
    ]
    is_lasts = [not bool(i) for i in tokens]

    _return = [(column_headers, rows)]

    return (
        _return
        if all(is_lasts)
        else _return + get_report(resource, builders, tokens, is_lasts)
    )


def _transform_report(column_header: ColumnHeader, rows: Row):
    dimension_header = [i.replace("ga:", "") for i in column_header["dimensions"]]
    metric_header = [
        i["name"].replace("ga:", "")
        for i in column_header["metricHeader"]["metricHeaderEntries"]
    ]

    dimension_values = [
        {
            k: datetime.strptime(v, "%Y%m%d").date().isoformat() if k == "date" else v
            for k, v in dict(zip(dimension_header, row["dimensions"])).items()  # type: ignore
        }
        for row in rows
    ]
    metric_values = [
        {k: round(float(v), 6) for k, v in zip(metric_header, row["metrics"][0]["values"])} for row in rows  # type: ignore
    ]

    return [
        {
            **dimension_value,
            **metric_value,
        }
        for dimension_value, metric_value in zip(dimension_values, metric_values)
    ]


def transform_page(report_page: ReportRes):
    return [
        _transform_report(column_header, row)
        for column_header, row in [
            (column_headers, rows) for column_headers, rows in zip(*report_page)
        ]
    ]
