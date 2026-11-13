"""The connection config the bridge publishes, with and without an offline-after."""

from pydoover.cloud.processor.types import ConnectionConfig, ConnectionType

from legacy_bridge_common.utils import connection_config_for

LEGACY = {"connectionType": "periodic", "connectionPeriod": 3720, "nextConnection": 14400}


def test_legacy_info_alone_maps_as_before():
    config = connection_config_for(LEGACY, None, ConnectionConfig, ConnectionType)
    assert config == ConnectionConfig.from_v1(LEGACY)
    assert config.offline_after is None


def test_offline_after_is_laid_over_legacy_info():
    config = connection_config_for(LEGACY, 21600, ConnectionConfig, ConnectionType)
    assert config.connection_type is ConnectionType.periodic
    assert config.expected_interval == 3720
    assert config.offline_after == 21600
    # The same again compares equal, so the bridge does not republish each report.
    assert config == connection_config_for(LEGACY, 21600, ConnectionConfig, ConnectionType)


def test_no_legacy_info_publishes_nothing_unless_offline_after_is_set():
    assert connection_config_for(None, None, ConnectionConfig, ConnectionType) is None
    config = connection_config_for(None, 10800, ConnectionConfig, ConnectionType)
    assert config.connection_type is ConnectionType.periodic_continuous
    assert config.offline_after == 10800
    assert config.expected_interval is None
