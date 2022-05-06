from typing import Union, Optional

from datetime import datetime
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


def get_service(reports: list[Report]):
    def _svc(start, end) -> list[ReportRes]:
        reports_length = len(reports)

        builders = [
            build_request(report.dimensions, report.metrics, start, end)
            for report in reports
        ]

        return get_report(
            get_resource(),
            builders,
            [None] * reports_length,
            [False] * reports_length,
        )

    return _svc


def _transform_service(report_res_pages: list[ReportRes]):
    transformed_pages = [transform_page(page) for page in report_res_pages]
    transformed_reports = [list(i) for i in zip(*transformed_pages)]
    with_batched_at = [
        [
            {
                **row,
                "_batched_at": datetime.utcnow().isoformat(timespec="seconds"),
            }
            for row in report
        ]
        for report in transformed_reports
    ]
    return with_batched_at


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
