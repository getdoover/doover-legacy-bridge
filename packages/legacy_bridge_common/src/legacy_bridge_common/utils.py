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
        for child in payload["children"].values():
            if isinstance(child, dict):
                nested_find_replace(child, key, old, new)
        # nested_find_replace(payload["children"], key, old, new)

    return payload


def nested_apply_to_component(payload, match_url, updates):
    """Recursively walk the widget tree and merge ``updates`` into every element
    whose ``componentUrl`` equals ``match_url``.

    Unlike ``nested_find_replace`` (which only swaps a single string value), this
    can also add *sibling* keys such as ``scope`` and ``module``. Those are
    required when a remote component is served from a Doover 2.0 channel instead
    of a static GitHub Pages URL: in channel mode the frontend would otherwise
    derive the Module Federation scope/module from the channel name, which does
    not match the widget's actual container name.
    """
    if not isinstance(payload, dict):
        return payload

    if payload.get("componentUrl") == match_url:
        payload.update(updates)

    children = payload.get("children")
    if isinstance(children, dict):
        for child in children.values():
            nested_apply_to_component(child, match_url, updates)

    return payload


# Channel-hosted widget definitions for the Zamil fuel-additive skids. The
# bundles are published to per-agent Doover channels via `doover channel
# publish-file` (channels: fuel_additive_widget, fuel_additive_hmi). Here the
# componentUrl is the *channel name* (no scheme), and scope/module pin the
# Module Federation container that lives inside each bundle.
_FUEL_ADDITIVE_WIDGET = {
    "componentUrl": "fuel_additive_widget",
    "scope": "FuelAdditiveWidget",
    "module": "./FuelAdditiveWidget",
}
_FUEL_ADDITIVE_HMI = {
    "componentUrl": "fuel_additive_hmi",
    "scope": "HMIComponent",
    "module": "./HMIComponent",
}

# Source componentUrl values that should resolve to FuelAdditiveWidget. Covers
# the original 1.0 reconciliation URL, the previously-swapped GitHub Pages widget
# URL, and the bare channel names a skid may already report. Matching all of them
# makes the mapping correct regardless of what the device sends and idempotent
# across re-syncs.
_FUEL_ADDITIVE_WIDGET_SOURCES = (
    "https://spaneng.github.io/fuel-additive-reconciliation/ReconciliationComponent.js",
    "https://spaneng.github.io/fuel-additive-widget/FuelAdditiveWidget.js",
    "fuel_additive_reconciliation",
    "fuel_additive_widget",
)

# Source componentUrl values that should resolve to the HMI widget.
_FUEL_ADDITIVE_HMI_SOURCES = (
    "https://spaneng.github.io/fuel-additive-hmi/HMIComponent.js",
    "fuel_additive_hmi",
)


def replace_widget_urls(state):
    nested_find_replace(
        state,
        "componentUrl",
        "https://getdoover.github.io/cameras/HLSLiveView.js",
        "https://getdoover.github.io/cameras/LiveViewV2.js",
    )

    # Reconciliation / fuel-additive dashboard -> FuelAdditiveWidget on a channel.
    for src in _FUEL_ADDITIVE_WIDGET_SOURCES:
        nested_apply_to_component(state, src, _FUEL_ADDITIVE_WIDGET)

    # HMI -> served from the fuel_additive_hmi channel.
    for src in _FUEL_ADDITIVE_HMI_SOURCES:
        nested_apply_to_component(state, src, _FUEL_ADDITIVE_HMI)

    # NOTE: doover_tables (DooverTables.js) is intentionally left on GitHub Pages.
    #
    # To render the standalone ReconciliationComponent instead of FuelAdditiveWidget,
    # drop "fuel_additive_reconciliation" from _FUEL_ADDITIVE_WIDGET_SOURCES and map
    # it separately to:
    #     {"componentUrl": "fuel_additive_reconciliation",
    #      "scope": "ReconciliationComponent",
    #      "module": "./ReconciliationComponent"}


def normalize_reported_desired(payload: dict) -> dict | None:
    """Flatten reported/desired ui_state into the children format 2.0 expects.

    If state has a ``reported`` key, its children are promoted to ``state.children``
    and the ``desired`` dict (the 1.0 equivalent of ui_cmds) is returned so the
    caller can sync it separately.  Returns ``None`` when the payload is already
    in the flat format.
    """
    state = payload.get("state")
    if not isinstance(state, dict) or "reported" not in state:
        return None

    reported = state.pop("reported")
    desired = state.pop("desired", None)
    state["children"] = reported.get("children", {})
    return desired

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

        if "ranges" in payload and isinstance(payload["ranges"], list):
            for r in payload["ranges"]:
                if isinstance(r, dict) and "showOnGraph" in r:
                    r["show_on_graph"] = r.pop("showOnGraph")

        # Recurse through all values in this dict
        for key, value in payload.items():
            if isinstance(value, dict):
                replace_units_add_requires_confirm(value)

        if payload.get("type") == "uiCamera":
            cam_name = payload.get("name") or "camera"
            payload["type"] = "uiSubmodule"
            payload["icon"] = "camera"
            payload["children"] = {
                cam_name: {
                    "type": "uiCameraHistory",
                    "cameraName": cam_name,
                    "displayName": payload.get("displayName"),
                    "name": cam_name,
                }
            }

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
