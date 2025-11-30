from pathlib import Path

from pydoover.cloud.processor import IngestionEndpointConfig
from pydoover.config import Schema, Array, String


# fixme: remove this when it's merged into pydoover
class DeviceString(String):
    def to_dict(self):
        data = super().to_dict()
        data["format"] = "doover-device"
        return data

class DevicesConfig(Array):
    def __init__(
        self,
        display_name: str = "Devices",
        *,
        description: str = "A list of devices to generate reports for.",
        **kwargs,
    ):
        element = DeviceString("Device", pattern="\d+")
        element._name = "dv_proc_devices"
        super().__init__(
            display_name, element=element, description=description, **kwargs
        )
        self._name = "dv_proc_devices"


class DooverLegacyBridgeConfig(Schema):
    def __init__(self):
        self.integration = IngestionEndpointConfig()
        self.devices = DevicesConfig()


def export():
    DooverLegacyBridgeConfig().export(
        Path(__file__).parents[2] / "doover_config.json",
        "doover_legacy_bridge_integration",
    )
