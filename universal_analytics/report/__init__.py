from universal_analytics.report import acquisitions

REPORTS = {
    report.name: report
    for report in [
        m.report
        for m in [
            acquisitions,
        ]
    ]
}
