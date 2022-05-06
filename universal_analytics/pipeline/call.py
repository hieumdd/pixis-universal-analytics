import json

from universal_analytics.pipeline.interface import Pipeline, CREATE_TIME
from universal_analytics.repo import get_listing, create_params_fn

from utils.utils import parse_unix_ts

pipeline = lambda name, params: Pipeline(
    name,
    get_listing("call"),
    lambda rows: [
        {
            "_id": row.get("_id"),
            "recordingDuration": row.get("recordingDuration"),
            "chargeTime": parse_unix_ts(row.get("chargeTime")),
            "flexChargeTime": row.get("flexChargeTime"),
            "transcripts": [i for i in row["transcripts"]]
            if row.get("transcripts")
            else [],
            "createTime": parse_unix_ts(row.get("createTime")),
            "direction": row.get("direction"),
            "fromCallId": row.get("fromCallId"),
            "toCallId": row.get("toCallId"),
            "localIp": row.get("localIp"),
            "fromExt": row.get("fromExt"),
            "fromNumber": row.get("fromNumber"),
            "toNumber": row.get("toNumber"),
            "startTime": parse_unix_ts(row.get("startTime")),
            "answerTime": parse_unix_ts(row.get("answerTime")),
            "endTime": parse_unix_ts(row.get("endTime")),
            "duration": row.get("duration"),
            "billDuration": row.get("billDuration"),
            "hangupCause": row.get("hangupCause"),
            "client": row.get("client"),
            "host": row.get("host"),
            "fromUser": {
                "_id": row["fromUser"].get("_id"),
                "active": row["fromUser"].get("active"),
                "role": row["fromUser"].get("role"),
                "updateTime": parse_unix_ts(row["fromUser"].get("updateTime")),
                "lastOnlineTime": parse_unix_ts(row["fromUser"].get("lastOnlineTime")),
                "client": row["fromUser"].get("client"),
                "timezone": row["fromUser"].get("timezone"),
                "group": row["fromUser"].get("group"),
                "email": row["fromUser"].get("email"),
                "ext": row["fromUser"].get("ext"),
                "extPassword": row["fromUser"].get("extPassword"),
                "name": row["fromUser"].get("name"),
                "phone": row["fromUser"].get("phone"),
                "language": row["fromUser"].get("language"),
                "createTime": parse_unix_ts(row["fromUser"].get("createTime")),
                "__v": row["fromUser"].get("__v"),
                "socketId": row["fromUser"].get("socketId"),
                "phoneIdle": row["fromUser"].get("phoneIdle"),
            }
            if row.get("fromUser")
            else {},
            "fromGroup": json.dumps(row.get("fromGroup")),
            "phoneNumber": row.get("phoneNumber"),
            "network": row.get("network"),
            "gateway": row.get("gateway"),
            "recording": row.get("recording"),
            "audioQuality": row.get("audioQuality"),
            "__v": row.get("__v"),
            "campaign": {
                "_id": row["campaign"].get("_id"),
                "startTime": row["campaign"].get("startTime"),
                "endTime": row["campaign"].get("endTime"),
                "retryDelay": row["campaign"].get("retryDelay"),
                "users": [i for i in row["campaign"]["users"]]
                if row["campaign"].get("users")
                else [],
                "managers": [i for i in row["campaign"]["managers"]]
                if row["campaign"].get("managers")
                else [],
                "active": row["campaign"].get("active"),
                "overtimeCall": row["campaign"].get("overtimeCall"),
                "autoCreateOpp": row["campaign"].get("autoCreateOpp"),
                "autoCreateCustomer": row["campaign"].get("autoCreateCustomer"),
                "oppAssignType": row["campaign"].get("oppAssignType"),
                "assignedUsers": [i for i in row["campaign"]["assignedUsers"]]
                if row["campaign"].get("assignedUsers")
                else [],
                "oppAssignStrategy": row["campaign"].get("oppAssignStrategy"),
                "contactCustomFields": [
                    {
                        "inputType": custom_field.get("inputType"),
                        "inputOptions": [i for i in custom_field["inputOptions"]]
                        if custom_field.get("inputOptions")
                        else [],
                        "requiredOnStatus": [
                            i for i in custom_field["requiredOnStatus"]
                        ]
                        if custom_field.get("requiredOnStatus")
                        else [],
                        "_id": custom_field.get("_id"),
                        "label": custom_field.get("label"),
                        "key": custom_field.get("key"),
                        "isOutputData": custom_field.get("isOutputData"),
                        "refKey": custom_field.get("refKey"),
                    }
                    for custom_field in row["campaign"]["contactCustomFields"]
                ]
                if row["campaign"].get("contactCustomFields")
                else [],
                "createTime": parse_unix_ts(row["campaign"].get("createTime")),
                "updateTime": parse_unix_ts(row["campaign"].get("updateTime")),
                "client": row["campaign"].get("client"),
                "name": row["campaign"].get("name"),
            }
            if row.get("campaign")
            else {},
            "recordingDownloaded": row.get("recordingDownloaded"),
            "recordingSize": row.get("recordingSize"),
            "chargeDuration": row.get("chargeDuration"),
            "chargedFee": row.get("chargedFee"),
            "offNetwork": row.get("offNetwork"),
            "id": row.get("id"),
        }
        for row in rows
    ],
    [
        {"name": "_id", "type": "STRING"},
        {"name": "recordingDuration", "type": "NUMERIC"},
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
        {"name": "client", "type": "STRING"},
        {"name": "host", "type": "STRING"},
        {
            "name": "fromUser",
            "type": "RECORD",
            "fields": [
                {"name": "_id", "type": "STRING"},
                {"name": "active", "type": "BOOLEAN"},
                {"name": "role", "type": "STRING"},
                {"name": "updateTime", "type": "TIMESTAMP"},
                {"name": "lastOnlineTime", "type": "TIMESTAMP"},
                {"name": "client", "type": "STRING"},
                {"name": "timezone", "type": "STRING"},
                {"name": "group", "type": "STRING"},
                {"name": "email", "type": "STRING"},
                {"name": "ext", "type": "STRING"},
                {"name": "extPassword", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
                {"name": "language", "type": "STRING"},
                {"name": "createTime", "type": "TIMESTAMP"},
                {"name": "__v", "type": "NUMERIC"},
                {"name": "socketId", "type": "STRING"},
                {"name": "phoneIdle", "type": "BOOLEAN"},
            ],
        },
        {"name": "fromGroup", "type": "STRING"},
        {"name": "phoneNumber", "type": "STRING"},
        {"name": "network", "type": "STRING"},
        {"name": "gateway", "type": "STRING"},
        {"name": "recording", "type": "BOOLEAN"},
        {"name": "audioQuality", "type": "NUMERIC"},
        {"name": "__v", "type": "NUMERIC"},
        {
            "name": "campaign",
            "type": "RECORD",
            "fields": [
                {"name": "_id", "type": "STRING"},
                {"name": "startTime", "type": "NUMERIC"},
                {"name": "endTime", "type": "NUMERIC"},
                {"name": "retryDelay", "type": "NUMERIC"},
                {"name": "users", "type": "STRING", "mode": "REPEATED"},
                {"name": "managers", "type": "STRING", "mode": "REPEATED"},
                {"name": "active", "type": "BOOLEAN"},
                {"name": "overtimeCall", "type": "BOOLEAN"},
                {"name": "autoCreateOpp", "type": "BOOLEAN"},
                {"name": "autoCreateCustomer", "type": "BOOLEAN"},
                {"name": "oppAssignType", "type": "STRING"},
                {"name": "assignedUsers", "type": "STRING", "mode": "REPEATED"},
                {"name": "oppAssignStrategy", "type": "STRING"},
                {
                    "name": "contactCustomFields",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "inputType", "type": "STRING"},
                        {"name": "inputOptions", "type": "STRING", "mode": "REPEATED"},
                        {
                            "name": "requiredOnStatus",
                            "type": "STRING",
                            "mode": "REPEATED",
                        },
                        {"name": "_id", "type": "STRING"},
                        {"name": "label", "type": "STRING"},
                        {"name": "key", "type": "STRING"},
                        {"name": "isOutputData", "type": "BOOLEAN"},
                        {"name": "refKey", "type": "STRING"},
                    ],
                },
                {"name": "createTime", "type": "TIMESTAMP"},
                {"name": "updateTime", "type": "TIMESTAMP"},
                {"name": "client", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "dialer", "type": "STRING"},
                {"name": "maxRetry", "type": "NUMERIC"},
                {"name": "__v", "type": "NUMERIC"},
                {"name": "autoReportOk", "type": "BOOLEAN"},
                {"name": "phoneNumber", "type": "STRING"},
            ],
        },
        {"name": "recordingDownloaded", "type": "BOOLEAN"},
        {"name": "recordingSize", "type": "NUMERIC"},
        {"name": "chargeDuration", "type": "NUMERIC"},
        {"name": "chargedFee", "type": "NUMERIC"},
        {"name": "offNetwork", "type": "BOOLEAN"},
        {"name": "id", "type": "STRING"},
    ],
    cursor_key=CREATE_TIME,
    params_fn=create_params_fn(params),
)
