from pydoover import config


class DooverLegacyBridgeCommonConfig(config.Schema):
    legacy_api_key = config.String("Legacy API Key")
    legacy_api_url = config.String("Legacy API URL")
