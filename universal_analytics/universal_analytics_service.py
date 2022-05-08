from typing import Union, Optional, Any

from datetime import datetime, timedelta, date
from compose import compose
from google.cloud.bigquery import LoadJob

from universal_analytics.report import REPORTS
from universal_analytics.repo import (
    get_resource,
    build_request,
    get_report,
    transform_page,
    ReportRes,
)
from db.bigquery import load

DATE_FORMAT = "%Y-%m-%d"


def _get_timeframe(input_: Optional[str], days: int) -> date:
    return (
        datetime.strptime(input_, "%Y-%m-%d").date()
        if input_
        else (datetime.utcnow() - timedelta(days=days)).date()
    )


def _get_service(timeframe: tuple[Optional[str], Optional[str]]) -> list[ReportRes]:
    start, end = timeframe
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


def _load_service(rowss: list[list[dict[str, Any]]]) -> list[int]:
    def _check_running(jobs: list[LoadJob]):
        is_dones = [job.done() for job in jobs]
        return [job.result() for job in jobs] if all(is_dones) else _check_running(jobs)

    jobs = [
        load(rows, pipeline.name, pipeline.schema)
        for rows, pipeline in zip(rowss, REPORTS)
    ]
    return [job.output_rows for job in _check_running(jobs)]


def pipeline_service(
    start: Optional[str],
    end: Optional[str],
) -> dict[str, Union[str, int]]:
    return compose(
        lambda x: [
            {
                "table": report.name,
                "start": start,
                "end": end,
                "output_rows": output_row,
            }
            for output_row, report in zip(x, REPORTS)
        ],
        _load_service,
        _transform_service,
        _get_service,
    )((start, end))
