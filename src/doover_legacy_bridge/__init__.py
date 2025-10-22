from pydoover.docker import run_app

from .application import DooverLegacyBridgeApplication
from .app_config import DooverLegacyBridgeConfig

def main():
    """
    Run the application.
    """
    run_app(DooverLegacyBridgeApplication(config=DooverLegacyBridgeConfig()))
