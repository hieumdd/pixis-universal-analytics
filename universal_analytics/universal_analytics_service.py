from typing import Union, Optional

from datetime import datetime, timedelta, date
from compose import compose

from universal_analytics.report.interface import Report
from universal_analytics.report import REPORTS
from universal_analytics.repo import (
    get_resource,
    build_request,
    get_report,
    transform_page,
    ReportRes,
)
from db.bigquery import load, update

DATE_FORMAT = "%Y-%m-%d"


def _get_timeframe(input_: Optional[str], days: int) -> date:
    return (
        datetime.strptime(input_, "%Y-%m-%d").date()
        if input_
        else (datetime.utcnow() - timedelta(days=days)).date()
    )


def _get_service(start: Optional[str], end: Optional[str]) -> list[ReportRes]:
    _start, _end = [
        _get_timeframe(input_, fallback)
        for input_, fallback in (
            (start, 2),
            (end, 0),
        )
    ]
    reports_length = len(REPORTS)

    builders = [
        build_request(report.dimensions, report.metrics, _start, _end)
        for report in REPORTS
    ]

    return get_report(
        get_resource(),
        builders,
        [None] * reports_length,
        [False] * reports_length,
    )


def _transform_service(report_res_pages: list[ReportRes]):
    pages = [transform_page(page) for page in report_res_pages]
    reports = [[i for j in report for i in j] for report in zip(*pages)]
    return [
        [
            {
                **row,
                "_batched_at": datetime.utcnow().isoformat(timespec="seconds"),
            }
            for row in report
        ]
        for report in reports
    ]


# def pipeline_service(
#     start: Optional[str],
#     end: Optional[str],
# ) -> dict[str, Union[str, int]]:
#     return compose(
#         lambda x: {
#             "table": pipeline.name,
#             "start": start,
#             "end": end,
#             "output_rows": x,
#         },
#         load(
#             pipeline.name,
#             pipeline.schema,
#             update(pipeline.id_key, pipeline.cursor_key),
#         ),
#         pipeline.transform,
#         pipeline.get,
#         pipeline.params_fn(pipeline.name, pipeline.cursor_key),
#     )((start, end))
