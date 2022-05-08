from ua import ua_service


def ua_controller(body: dict[str, str]):
    return ua_service.pipeline_service(
        body.get("start"),
        body.get("end"),
    )
