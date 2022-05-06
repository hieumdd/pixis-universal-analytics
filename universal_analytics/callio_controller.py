from universal_analytics.pipeline import pipelines
from universal_analytics import universal_analytics_service


def callio_controller(body: dict[str, str]):
    return universal_analytics_service.pipeline_service(
        pipelines[body.get("table", "")],
        body.get("start"),
        body.get("end"),
    )
