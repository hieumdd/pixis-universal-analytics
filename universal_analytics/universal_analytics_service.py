from typing import Union, Optional

from datetime import datetime
from compose import compose

from universal_analytics.report.interface import Report
from universal_analytics.report import REPORTS
from universal_analytics.repo import get_resource, build_request, get_report, ReportRes
from db.bigquery import load, update


def get_service(reports: list[Report]):
    def _svc(start, end):
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


def _transform_service(reports: list[Report]):
    def _transform(report_res):
        column_header, row = report_res

        dimension_header = [i.replace("ga:", "") for i in column_header["dimensions"]]
        metric_header = [
            i["name"].replace("ga:", "")
            for i in column_header["metricHeader"]["metricHeaderEntries"]
        ]

        dimension_values = {
            k: datetime.strptime(v, "%Y%m%d").date().isoformat() if k == "date" else v
            for k, v in dict(zip(dimension_header, row["dimensions"])).items()
        }
        metric_values = dict(zip(metric_header, row["metrics"][0]["values"]))
        
        return {
            **dimension_values,
            **metric_values,
        }

    def _svc(report_res_pages: list[list[ReportRes]]):
        transformed_pages = [_transform(res) for page in report_res_pages for res in page]
        transformed = [list(i) for i in zip(*transformed_pages)]
        with_batched_at = [
            {
                **row,
                "_batched_at": datetime.utcnow().isoformat(timespec="seconds"),
            } for row in transformed
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
