from universal_analytics.pipeline.interface import Pipeline
from universal_analytics.repo import get_listing, create_params_fn

from utils.utils import parse_unix_ts

pipeline = Pipeline(
    "CallCampaignDial",
    get_listing("call-campaign-dial"),
    lambda rows: [
        {
            "_id": row.get("_id"),
            "phone": row.get("phone"),
            "name": row.get("name"),
            "callTime": parse_unix_ts(row.get("callTime")),
            "reportTime": parse_unix_ts(row.get("reportTime")),
            "finishTime": parse_unix_ts(row.get("finishTime")),
            "retryTime": parse_unix_ts(row.get("retryTime")),
            "needSync": row.get("needSync"),
            "flexChargeTime": row.get("flexChargeTime"),
            "customFields": [
                {
                    "key": custom_fields.get("key"),
                    "val": custom_fields.get("val"),
                    "_id": custom_fields.get("_id"),
                }
                for custom_fields in row["customFields"]
            ]
            if row.get("customFields")
            else [],
            "dials": [
                {
                    "user": dial.get("user"),
                    "userName": dial.get("userName"),
                    "userExt": dial.get("userExt"),
                    "userEmail": dial.get("userEmail"),
                    "retryTime": dial.get("retryTime"),
                    "callId": dial.get("callId"),
                    "group": dial.get("group"),
                    "reportTime": parse_unix_ts(dial.get("reportTime")),
                    "src": dial.get("src"),
                    "_id": dial.get("_id"),
                    "createTime": parse_unix_ts(dial.get("createTime")),
                    "customFields": [
                        {
                            "key": custom_fields.get("key"),
                            "val": custom_fields.get("val"),
                            "_id": custom_fields.get("_id"),
                        }
                        for custom_fields in dial["customFields"]
                    ]
                    if dial.get("customFields")
                    else [],
                    "status": dial.get("status"),
                    "billDuration": dial.get("billDuration"),
                    "call": {
                        "_id": dial["call"].get("_id"),
                        "recordingDuration": dial["call"].get("recordingDuration"),
                        "chargeTime": parse_unix_ts(dial["call"].get("chargeTime")),
                        "flexChargeTime": dial["call"].get("flexChargeTime"),
                        "transcripts": [i for i in dial["call"]["transcripts"]]
                        if dial["call"].get("transcripts")
                        else [],
                        "createTime": parse_unix_ts(dial["call"].get("createTime")),
                        "direction": dial["call"].get("direction"),
                        "fromCallId": dial["call"].get("fromCallId"),
                        "toCallId": dial["call"].get("toCallId"),
                        "localIp": dial["call"].get("localIp"),
                        "fromExt": dial["call"].get("fromExt"),
                        "fromNumber": dial["call"].get("fromNumber"),
                        "toNumber": dial["call"].get("toNumber"),
                        "startTime": parse_unix_ts(dial["call"].get("startTime")),
                        "answerTime": parse_unix_ts(dial["call"].get("answerTime")),
                        "endTime": parse_unix_ts(dial["call"].get("endTime")),
                        "duration": dial["call"].get("duration"),
                        "billDuration": dial["call"].get("billDuration"),
                        "hangupCause": dial["call"].get("hangupCause"),
                        "campaign": dial["call"].get("campaign"),
                        "client": dial["call"].get("client"),
                        "host": dial["call"].get("host"),
                        "fromUser": dial["call"].get("fromUser"),
                        "fromGroup": dial["call"].get("fromGroup"),
                        "phoneNumber": dial["call"].get("phoneNumber"),
                        "network": dial["call"].get("network"),
                        "gateway": dial["call"].get("gateway"),
                        "recording": dial["call"].get("recording"),
                        "audioQuality": dial["call"].get("audioQuality"),
                        "__v": dial["call"].get("__v"),
                        "recordingDownloaded": dial["call"].get("recordingDownloaded"),
                        "recordingSize": dial["call"].get("recordingSize"),
                        "chargeDuration": dial["call"].get("chargeDuration"),
                        "chargedFee": dial["call"].get("chargedFee"),
                        "offNetwork": dial["call"].get("offNetwork"),
                        "id": dial["call"].get("id"),
                    }
                    if dial.get("call")
                    else {},
                }
                for dial in row["dials"]
            ]
            if row.get("dials")
            else [],
            "createTime": parse_unix_ts(row.get("createTime")),
            "updateTime": parse_unix_ts(row.get("updateTime")),
            "client": row.get("client"),
            "campaign": {
                "_id": row["campaign"].get("_id"),
                "name": row["campaign"].get("name"),
                "id": row["campaign"].get("id"),
            }
            if row.get("campaign")
            else {},
            "__v": row.get("__v"),
            "user": row.get("user"),
            "status": row.get("status"),
        }
        for row in rows
    ],
    [
        {"name": "_id", "type": "STRING"},
        {"name": "phone", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "callTime", "type": "TIMESTAMP"},
        {"name": "reportTime", "type": "TIMESTAMP"},
        {"name": "finishTime", "type": "TIMESTAMP"},
        {"name": "retryTime", "type": "TIMESTAMP"},
        {"name": "needSync", "type": "BOOLEAN"},
        {"name": "flexChargeTime", "type": "NUMERIC"},
        {
            "name": "customFields",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "key", "type": "STRING"},
                {"name": "val", "type": "STRING", "mode": "REPEATED"},
                {"name": "_id", "type": "STRING"},
            ],
        },
        {
            "name": "dials",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "user", "type": "STRING"},
                {"name": "userName", "type": "STRING"},
                {"name": "userExt", "type": "STRING"},
                {"name": "userEmail", "type": "STRING"},
                {"name": "retryTime", "type": "NUMERIC"},
                {"name": "callId", "type": "STRING"},
                {"name": "group", "type": "STRING"},
                {"name": "reportTime", "type": "TIMESTAMP"},
                {"name": "src", "type": "NUMERIC"},
                {"name": "_id", "type": "STRING"},
                {"name": "createTime", "type": "TIMESTAMP"},
                {
                    "name": "customFields",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "key", "type": "STRING"},
                        {"name": "val", "type": "STRING", "mode": "REPEATED"},
                        {"name": "_id", "type": "STRING"},
                    ],
                },
                {"name": "status", "type": "STRING"},
                {"name": "billDuration", "type": "NUMERIC"},
                {
                    "name": "call",
                    "type": "RECORD",
                    "fields": [
                        {"name": "_id", "type": "STRING"},
                        {"name": "recordingDuration", "type": "FLOAT"},
                        {"name": "chargeTime", "type": "TIMESTAMP"},
                        {"name": "flexChargeTime", "type": "NUMERIC"},
                        {"name": "transcripts", "type": "STRING", "mode": "REPEATED"},
                        {"name": "createTime", "type": "TIMESTAMP"},
                        {"name": "direction", "type": "STRING"},
                        {"name": "fromCallId", "type": "STRING"},
                        {"name": "toCallId", "type": "STRING"},
                        {"name": "localIp", "type": "STRING"},
                        {"name": "fromExt", "type": "STRING"},
                        {"name": "fromNumber", "type": "STRING"},
                        {"name": "toNumber", "type": "STRING"},
                        {"name": "startTime", "type": "TIMESTAMP"},
                        {"name": "answerTime", "type": "TIMESTAMP"},
                        {"name": "endTime", "type": "TIMESTAMP"},
                        {"name": "duration", "type": "NUMERIC"},
                        {"name": "billDuration", "type": "NUMERIC"},
                        {"name": "hangupCause", "type": "STRING"},
                        {"name": "campaign", "type": "STRING"},
                        {"name": "client", "type": "STRING"},
                        {"name": "host", "type": "STRING"},
                        {"name": "fromUser", "type": "STRING"},
                        {"name": "fromGroup", "type": "STRING"},
                        {"name": "phoneNumber", "type": "STRING"},
                        {"name": "network", "type": "STRING"},
                        {"name": "gateway", "type": "STRING"},
                        {"name": "recording", "type": "BOOLEAN"},
                        {"name": "audioQuality", "type": "NUMERIC"},
                        {"name": "__v", "type": "NUMERIC"},
                        {"name": "recordingDownloaded", "type": "BOOLEAN"},
                        {"name": "recordingSize", "type": "NUMERIC"},
                        {"name": "chargeDuration", "type": "NUMERIC"},
                        {"name": "chargedFee", "type": "NUMERIC"},
                        {"name": "offNetwork", "type": "BOOLEAN"},
                        {"name": "id", "type": "STRING"},
                    ],
                },
                {"name": "duration", "type": "NUMERIC"},
                {"name": "hangupCause", "type": "STRING"},
            ],
        },
        {"name": "createTime", "type": "TIMESTAMP"},
        {"name": "updateTime", "type": "TIMESTAMP"},
        {"name": "client", "type": "STRING"},
        {
            "name": "campaign",
            "type": "RECORD",
            "fields": [
                {"name": "_id", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "id", "type": "STRING"},
            ],
        },
        {"name": "__v", "type": "NUMERIC"},
        {"name": "user", "type": "STRING"},
        {"name": "status", "type": "STRING"},
    ],
    params_fn=create_params_fn(),
)
