from pathlib import Path

from pydoover import config
from pydoover.processor import ManySubscriptionConfig

from legacy_bridge_common import DooverLegacyBridgeCommonConfig


class DooverLegacyBridgeConfig(DooverLegacyBridgeCommonConfig):
    subscription = ManySubscriptionConfig()

    legacy_agent_key = config.String("Agent Key")
    read_only = config.Boolean(
        "Read Only",
        description="If this is enabled, no messages will be written back to Doover 1.0, ever.",
        default=False,
    )

def export():
    DooverLegacyBridgeConfig.export(
        Path(__file__).parents[2] / "doover_config.json",
        "doover_legacy_bridge_cloud_app",
    )
