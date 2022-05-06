from typing import Callable, Any, Optional
from datetime import datetime

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

DATASET = "Callio"


def get_last_timestamp(table: str, time_key: str) -> datetime:
    rows = BQ_CLIENT.query(
        f"SELECT MAX({time_key}) AS incre FROM {DATASET}.{table}"
    ).result()
    return [row for row in rows][0]["incre"]


def load(
    table: str, schema: list[dict[str, Any]], update_fn: Callable[[str], None] = None
):
    def _load(data: list[dict[str, Any]]) -> int:
        if len(data) == 0:
            return 0

        output_rows = (
            BQ_CLIENT.load_table_from_json(  # type: ignore
                data,
                f"{DATASET}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND" if update_fn else "WRITE_TRUNCATE",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        if update_fn:
            update_fn(table)
        return output_rows

    return _load


def update(id_key: list[str], time_key: str):
    def _update(table: str):
        BQ_CLIENT.query(
            f"""
        CREATE OR REPLACE TABLE {DATASET}.{table} AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {','.join(id_key)} ORDER BY {time_key} DESC) AS row_num,
            FROM {DATASET}.{table}
        ) WHERE row_num = 1
        """
        ).result()

    return _update
