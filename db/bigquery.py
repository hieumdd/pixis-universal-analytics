from typing import Any

from google.cloud import bigquery

client = bigquery.Client()

DATASET = "UniversalAnalytics"


def load(
    data: list[dict[str, Any]],
    table: str,
    schema: list[dict[str, Any]],
) -> bigquery.LoadJob:
    return client.load_table_from_json(  # type: ignore
        data,
        f"{DATASET}.p_{table}",
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            schema=schema,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        ),
    )
