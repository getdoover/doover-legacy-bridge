from pathlib import Path

from pydoover.cloud.processor import IngestionEndpointConfig
from pydoover.cloud.processor.config import ExtendedPermissionsConfig
from pydoover.config import Schema


class DooverLegacyBridgeConfig(Schema):
    def __init__(self):
        self.integration = IngestionEndpointConfig()
        self.permissions = ExtendedPermissionsConfig()


def export():
    DooverLegacyBridgeConfig().export(
        Path(__file__).parents[2] / "doover_config.json",
        "doover_legacy_bridge_integration",
    )
