import base64
import logging
import mimetypes
from contextlib import suppress

log = logging.getLogger(__name__)

def get_connection_info(payload) -> dict | None:
    for k, v in payload.get("children", {}).items():
        if v.get("type") == "uiConnectionInfo":
            return v

        try:
            v["children"]
        except KeyError:
            pass
        else:
            conn_info = get_connection_info(v)
            if conn_info is not None:
                return conn_info

    return None


def parse_file(channel_name: str, payload: dict):
    log.info("Handling file message.")

    data = payload["output"]
    del payload["output"]

    file_type = payload["output_type"]

    data_bytes = base64.b64decode(data)

    match file_type:
        case "mp4":
            content_type = "video/mp4"
        case "jpg" | "jpeg":
            content_type = "image/jpeg"
        case other:
            content_type = other

    extension = mimetypes.guess_extension(content_type, strict=True)
    if not extension:
        # generic binary, this really shouldn't happen
        content_type = "application/octet-stream"
        extension = ""

    file = (f"{channel_name}{extension}", data_bytes, content_type)
    return payload, file


def nested_find_replace(payload, key, old, new):
    try:
        existing = payload[key]
    except KeyError:
        pass
    else:
        if existing == old:
            payload[key] = new

    if "children" in payload:
        nested_find_replace(payload["children"], key, old, new)

    return payload

def find_element(key, payload):
    try:
        return payload[key]
    except KeyError:
        pass

    for v in payload.values():
        if isinstance(v, dict):
            with suppress(KeyError):
                return find_element(key, v["children"])

            with suppress(KeyError):
                return find_element(key, v)

    raise KeyError(f"key '{key}' not found")
