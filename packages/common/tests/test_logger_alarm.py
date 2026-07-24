import os
import pytest

from common.logging import setup_logger
from common.config import settings

pytestmark = pytest.mark.skipif(
    not settings.discord_webhook_url,
    reason="DISCORD_WEBHOOK_URL not set in environment or config"
)

def test_centralized_logger_alarm():
    """Test if centralized logger alarm works properly."""
    logger = setup_logger("test_centralized_alarm")
    
    try:
        1 / 0
    except Exception as e:
        logger.error(f"Something broke: {e}", exc_info=True)
