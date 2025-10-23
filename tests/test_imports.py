"""
Basic tests for an application.

This ensures all modules are importable and that the config is valid.
"""

def test_import_app():
    from doover_legacy_bridge.application import DooverLegacyBridgeApplication
    assert DooverLegacyBridgeApplication

def test_config():
    from doover_legacy_bridge.app_config import DooverLegacyBridgeConfig

    config = DooverLegacyBridgeConfig()
    assert isinstance(config.to_dict(), dict)
