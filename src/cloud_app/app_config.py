from pathlib import Path

from pydoover import config
from pydoover.cloud.processor import ManySubscriptionConfig

from legacy_bridge_common import DooverLegacyBridgeCommonConfig


class DooverLegacyBridgeConfig(DooverLegacyBridgeCommonConfig):
    def __init__(self):
        super().__init__()
        # we don't really need this inheritance, I just wanted to showcase how to use uv workspaces / multiple apps
        # in one repo.
        self.subscription = ManySubscriptionConfig()

        self.legacy_agent_key = config.String("Agent Key")
        self.read_only = config.Boolean(
            "Read Only",
            description="If this is enabled, no messages will be written back to Doover 1.0, ever.",
            default=False,
        )
        self.offline_after = config.Integer(
            "Offline After",
            default=None,
            minimum=60,
            description=(
                "Seconds after the device's last report before Doover 2.0 marks it offline. "
                "Set this when the device's own connection settings understate how often it "
                "really reports, or when its Doover 1.0 socket drops and reconnects on a cycle. "
                "Leave blank to use the device's own settings."
            ),
        )


def export():
    DooverLegacyBridgeConfig().export(
        Path(__file__).parents[2] / "doover_config.json",
        "doover_legacy_bridge_cloud_app",
    )
