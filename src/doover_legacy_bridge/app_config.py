from pathlib import Path

from pydoover import config


class DooverLegacyBridgeConfig(config.Schema):
    def __init__(self):
        self.legacy_agent_key = config.String("Agent Key")


def export():
    DooverLegacyBridgeConfig().export(Path(__file__).parents[2] / "doover_config.json", "doover_legacy_bridge")

if __name__ == "__main__":
    export()
