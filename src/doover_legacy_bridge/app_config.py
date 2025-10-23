from pathlib import Path

from pydoover import config
from pydoover.cloud.processor import ScheduleConfig, ManySubscriptionConfig


class DooverLegacyBridgeConfig(config.Schema):
    def __init__(self):
        self.legacy_agent_key = config.String("Agent Key")
        self.legacy_api_key = config.String("Legacy API Key")
        self.legacy_api_url = config.String("Legacy API URL")
        self.subscription = ManySubscriptionConfig()
        self.schedule = ScheduleConfig()


def export():
    DooverLegacyBridgeConfig().export(Path(__file__).parents[2] / "doover_config.json", "doover_legacy_bridge")

if __name__ == "__main__":
    export()
