"""Basic tests to ensure all modules are importable and configs are valid."""


def test_import_cloud_app():
    from cloud_app.application import DooverLegacyBridgeApplication
    assert DooverLegacyBridgeApplication


def test_import_integration():
    from integration.application import DooverLegacyBridgeApplication
    assert DooverLegacyBridgeApplication


def test_cloud_app_config():
    from cloud_app.app_config import DooverLegacyBridgeConfig

    DooverLegacyBridgeConfig.clear_elements()
    config = DooverLegacyBridgeConfig()
    assert isinstance(config.to_dict(), dict)


def test_integration_config():
    from integration.app_config import DooverLegacyBridgeConfig

    DooverLegacyBridgeConfig.clear_elements()
    config = DooverLegacyBridgeConfig()
    assert isinstance(config.to_dict(), dict)
