from pathlib import Path

from pydoover.processor import IngestionEndpointConfig, ExtendedPermissionsConfig
from pydoover.config import Schema


class DooverLegacyBridgeConfig(Schema):
    integration = IngestionEndpointConfig()
    permissions = ExtendedPermissionsConfig()


def export():
    DooverLegacyBridgeConfig.export(
        Path(__file__).parents[2] / "doover_config.json",
        "doover_legacy_bridge_integration",
    )
