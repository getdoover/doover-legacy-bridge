import json
import os

os.environ.update(
    **{
        "APP_KEY": "test-processor",
        "DOOVER_DATA_ENDPOINT": "https://data.staging.udoover.com/api",
    }
)

from doover_legacy_bridge import handler

payload = {
    "token": "",
    "op": "on_message_create",
    "agent_id": "106957929290186767",
    "d": {
        "owner_id": 106957929290186767,
        "channel_name": "ui_cmds",
        "author_id": 7368462157945913344,
        "message": {
            "id": 106957929290186883,
            "author_id": 106957929290186767,
            "data": {"test": 1},
        },
    },
}

handler({"Records": [{
    "EventSource": "aws:sns",
    "Sns": {"Message": json.dumps(payload)},
    "EventSubscriptionArn": ""
}]}, {})
