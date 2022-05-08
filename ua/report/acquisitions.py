from ua.report.interface import Report

report = Report(
    "Acquisitions",
    [
        "date",
        "channelGrouping",
        "sourceMedium",
        "source",
        "medium",
        "deviceCategory",
    ],
    [
        "users",
        "newUsers",
        "sessionsPerUser",
        "sessions",
        "pageviews",
        "pageviewsPerSession",
        "avgSessionDuration",
        "bounceRate",
    ],
)
