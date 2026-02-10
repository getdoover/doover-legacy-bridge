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


def replace_units_add_requires_confirm(payload):
    if isinstance(payload, dict):
        # Check if this dict has a displayString with units
        if "displayString" in payload:
            name = payload["displayString"]
            if "(" in name and ")" in name:
                new, units = name.split("(", 1)  # Split only on first (
                payload["displayString"] = new.strip()
                payload["units"] = units.strip().rstrip(")")

        if "type" in payload and "requiresConfirm" not in payload:
            if payload["type"] in ("uiStateCommand", "uiSlider", "uiInteraction"):
                payload["requiresConfirm"] = True

        # Recurse through all values in this dict
        for key, value in payload.items():
            if isinstance(value, dict):
                replace_units_add_requires_confirm(value)

    return payload


def _transform_ranges(ranges: list) -> list:
    """Convert old range format (showOnGraph) to new format (show_on_graph)."""
    return [
        {
            **(
                {k: v for k, v in r.items() if k != "showOnGraph"}
                | ({"show_on_graph": r["showOnGraph"]} if "showOnGraph" in r else {})
            )
        }
        for r in ranges
    ]


def _transform_element(element: dict, position: int) -> dict:
    """Transform a single UI element from old to new format."""
    elem_type = element.get("type")
    transformed = {**element}

    transformed["position"] = position

    if "hidden" not in transformed:
        transformed["hidden"] = False

    if elem_type == "uiVariable":
        if "showActivity" not in transformed:
            transformed["showActivity"] = True
        if "ranges" in transformed:
            transformed["ranges"] = _transform_ranges(transformed["ranges"])
        else:
            transformed["ranges"] = []

    elif elem_type == "uiAction":
        if "disabled" not in transformed:
            transformed["disabled"] = False
        if "requiresConfirm" not in transformed:
            transformed["requiresConfirm"] = False

    elif elem_type == "uiSubmodule":
        if "showActivity" not in transformed:
            transformed["showActivity"] = True
        if "children" in transformed:
            new_sub_children = {}
            sub_pos = 101
            for child_key, child in transformed["children"].items():
                new_sub_children[child_key] = _transform_element(child, sub_pos)
                sub_pos += 1
            transformed["children"] = new_sub_children

    return transformed


def transform_legacy_ui_schema(
    ui_state: dict,
    ui_cmds: dict | None = None,
    app_key: str = "legacy_app",
    app_display_name: str = "Sensor",
) -> dict:
    """
    Transform old Doover 1.0 UI schema into the new Doover 2.0 format.

    Old ui_state (reported): {
        "children": { ... },
        "name": "uuid",
        "statusIcon": "idle",
        "type": "uiContianer"
    }
    Old ui_cmds (desired): {
        "low_power_voltage": 12.1,
        "sleep_period_secs": 1500,
        ...
    }

    New format: {
        "state": {
            "children": {
                "app_key": {
                    "type": "uiApplication",
                    "displayString": "Sensor",
                    "children": { ... }
                }
            }
        }
    }

    Args:
        ui_state: The old reported state (uiContianer with children).
        ui_cmds: The old desired state / commands dict. If provided,
                 any keys matching uiHiddenValue entries in ui_state will
                 be turned into uiSlider elements.
        app_key: Key to use for the wrapping uiApplication.
        app_display_name: Display name for the wrapping uiApplication.
    """
    old_children = ui_state.get("children", {})

    new_children = {}
    connection_info = None
    hidden_keys = set()
    position = 101

    # First pass: identify hidden values and connection info
    for key, element in old_children.items():
        elem_type = element.get("type")
        if elem_type == "uiHiddenValue":
            hidden_keys.add(key)
        elif elem_type == "uiConnectionInfo":
            connection_info = {**element}

    # Second pass: transform visible elements
    for key, element in old_children.items():
        elem_type = element.get("type")

        if elem_type in ("uiHiddenValue", "uiConnectionInfo"):
            continue

        new_children[key] = _transform_element(element, position)
        position += 1

    # Convert hidden values with known desired values into sliders
    if ui_cmds:
        for key in hidden_keys:
            if key not in ui_cmds:
                continue
            value = ui_cmds[key]
            new_children[key] = {
                "name": key,
                "type": "uiSlider",
                "displayString": key.replace("_", " ").title(),
                "position": position,
                "hidden": False,
                "currentValue": value,
                "min": 0,
                "max": value * 2 if isinstance(value, (int, float)) else 100,
                "stepSize": 0.1 if isinstance(value, float) else 100,
                "dualSlider": False,
                "isInverted": False,
            }
            position += 1

    # Build application wrapper
    application = {
        "type": "uiApplication",
        "displayString": app_display_name,
        "showActivity": True,
        "hidden": False,
        "children": new_children,
    }

    if connection_info:
        application["children"]["connection_info"] = connection_info

    return {
        "state": {
            "children": {
                app_key: application,
            }
        }
    }
