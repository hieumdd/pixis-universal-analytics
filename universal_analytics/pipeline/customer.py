from universal_analytics.pipeline.interface import Pipeline
from universal_analytics.repo import get_listing

from utils.utils import parse_unix_ts

pipeline = Pipeline(
    "Customer",
    get_listing("customer"),
    lambda rows: [
        {
            "_id": row.get("_id"),
            "users": [i for i in row["users"]] if row.get("users") else [],
            "flexChargeTime": parse_unix_ts(row.get("flexChargeTime")),
            "createTime": parse_unix_ts(row.get("createTime")),
            "updateTime": parse_unix_ts(row.get("updateTime")),
            "customFields": [
                {
                    "val": [i for i in custom_field["val"]]
                    if custom_field.get("val")
                    else [],
                    "_id": custom_field.get("_id"),
                    "key": custom_field.get("key"),
                }
                for custom_field in row["customFields"]
            ]
            if row.get("customFields")
            else [],
            "client": row.get("client"),
            "user": {
                "_id": row["user"].get("_id"),
                "email": row["user"].get("email"),
                "name": row["user"].get("name"),
                "ext": row["user"].get("ext"),
            }
            if row.get("user")
            else {},
            "desc": row.get("desc"),
            "name": row.get("name"),
            "__v": row.get("__v"),
            "mainContact": {
                "_id": row["mainContact"].get("_id"),
                "gender": row["mainContact"].get("gender"),
                "facebookId": str(row["mainContact"].get("facebookId")),
                "zaloId": str(row["mainContact"].get("zaloId")),
                "zaloName": str(row["mainContact"].get("zaloName")),
                "createTime": parse_unix_ts(row["mainContact"].get("createTime")),
                "client": row["mainContact"].get("client"),
                "customer": row["mainContact"].get("customer"),
                "phone": row["mainContact"].get("phone"),
                "name": row["mainContact"].get("name"),
                "__v": row["mainContact"].get("__v"),
            },
            "id": row.get("id"),
        }
        for row in rows
    ],
    [
        {"name": "_id", "type": "STRING"},
        {"name": "users", "type": "STRING", "mode": "REPEATED"},
        {"name": "flexChargeTime", "type": "TIMESTAMP"},
        {"name": "createTime", "type": "TIMESTAMP"},
        {"name": "updateTime", "type": "TIMESTAMP"},
        {
            "name": "customFields",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "val", "type": "STRING", "mode": "REPEATED"},
                {"name": "_id", "type": "STRING"},
                {"name": "key", "type": "STRING"},
            ],
        },
        {"name": "client", "type": "STRING"},
        {
            "name": "user",
            "type": "RECORD",
            "fields": [
                {"name": "_id", "type": "STRING"},
                {"name": "email", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "ext", "type": "STRING"},
            ],
        },
        {"name": "desc", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "__v", "type": "NUMERIC"},
        {
            "name": "mainContact",
            "type": "record",
            "fields": [
                {"name": "_id", "type": "STRING"},
                {"name": "gender", "type": "STRING"},
                {"name": "facebookId", "type": "STRING"},
                {"name": "zaloId", "type": "STRING"},
                {"name": "zaloName", "type": "STRING"},
                {"name": "createTime", "type": "TIMESTAMP"},
                {"name": "client", "type": "STRING"},
                {"name": "customer", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "__v", "type": "NUMERIC"},
            ],
        },
        {"name": "id", "type": "STRING"},
    ],
)
