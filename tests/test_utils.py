"""Unit tests for the shared ui_state/ui_cmds transforms.

The shadow-shaped payloads mirror real greengrass devices (e.g. the CQC Pile
composters), whose 1.0 ui_state is an AWS IoT shadow document: commands live in
``state.desired`` and the ui tree in ``state.reported`` - there is no ui_cmds
channel on the device at all.
"""

from legacy_bridge_common.utils import is_shadow_schema, normalize_reported_desired


def make_shadow_payload():
    return {
        "state": {
            "desired": {"aeratorState": "timer", "alertHighTemp": 76},
            "reported": {
                "type": "uiContianer",
                "name": "59558513-0e0a-423a-926e-e2b667e383b0",
                "children": {
                    "aeratorState": {"type": "uiStateCommand", "name": "aeratorState"},
                },
            },
        }
    }


def test_is_shadow_schema():
    assert is_shadow_schema(make_shadow_payload())
    assert not is_shadow_schema({"state": {"children": {}}})
    assert not is_shadow_schema({"state": {"desired": {"x": 1}}})
    assert not is_shadow_schema({})
    assert not is_shadow_schema({"state": "not-a-dict"})


def test_normalize_full_shadow_document():
    payload = make_shadow_payload()
    desired = normalize_reported_desired(payload)

    assert desired == {"aeratorState": "timer", "alertHighTemp": 76}
    assert payload == {
        "state": {
            "children": {
                "aeratorState": {"type": "uiStateCommand", "name": "aeratorState"},
            }
        }
    }


def test_normalize_reported_only_diff():
    payload = {"state": {"reported": {"children": {"lastTemp": {"currentValue": 55}}}}}
    desired = normalize_reported_desired(payload)

    assert desired is None
    assert payload == {"state": {"children": {"lastTemp": {"currentValue": 55}}}}


def test_normalize_desired_only_diff():
    # a 1.0-side user changing a setting on a shadow device publishes only desired
    payload = {"state": {"desired": {"aeratorState": "auto"}}}
    desired = normalize_reported_desired(payload)

    assert desired == {"aeratorState": "auto"}
    assert payload == {"state": {}}


def test_normalize_flat_payload_untouched():
    payload = {"state": {"children": {"foo": {"type": "uiVariable"}}}}
    desired = normalize_reported_desired(payload)

    assert desired is None
    assert payload == {"state": {"children": {"foo": {"type": "uiVariable"}}}}


def test_normalize_non_dict_state():
    assert normalize_reported_desired({}) is None
    assert normalize_reported_desired({"state": None}) is None
