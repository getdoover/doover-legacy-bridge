from typing import Any

from pydoover.cloud.processor import run_app

from .application import DooverLegacyBridgeApplication
from .app_config import DooverLegacyBridgeConfig

def handler(event: dict[str, Any], context):
    """
    Run the application.
    """
    EwonConfig.clear_elements()
    run_app(EwonApplication(config=EwonConfig()), event, context)
