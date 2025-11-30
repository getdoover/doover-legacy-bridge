from pydoover import config


class DooverLegacyBridgeCommonConfig(config.Schema):
    def __init__(self):
        self.legacy_api_key = config.String("Legacy API Key")
        self.legacy_api_url = config.String("Legacy API URL")
