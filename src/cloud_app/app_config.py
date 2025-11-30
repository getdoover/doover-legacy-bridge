from pathlib import Path

from pydoover import config
from pydoover.cloud.processor import ScheduleConfig, ManySubscriptionConfig

from legacy_bridge_common import DooverLegacyBridgeCommonConfig


class DooverLegacyBridgeConfig(DooverLegacyBridgeCommonConfig):
    def __init__(self):
        super().__init__()
        # we don't really need this inheritance, I just wanted to showcase how to use uv workspaces / multiple apps
        # in one repo.
        self.subscription = ManySubscriptionConfig()
        self.schedule = ScheduleConfig()

        self.legacy_agent_key = config.String("Agent Key")
        self.import_mode = config.Boolean("Import Mode")


def export():
    DooverLegacyBridgeConfig().export(
        Path(__file__).parents[2] / "doover_config.json",
        "doover_legacy_bridge_cloud_app",
    )
