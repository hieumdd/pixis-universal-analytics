from universal_analytics import universal_analytics_service


def universal_analytics_controller(body: dict[str, str]):
    return universal_analytics_service.pipeline_service(
        body.get("start"),
        body.get("end"),
    )
