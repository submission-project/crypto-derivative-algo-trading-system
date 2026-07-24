import os
import pytest
from common.alarm import Alarm
from common.config import settings

# Make sure this test only runs if explicitly desired or if discord URL is configured
# Otherwise it might spam the webhook during normal testing.
pytestmark = pytest.mark.skipif(
    not settings.discord_webhook_url,
    reason="DISCORD_WEBHOOK_URL not set in environment or config"
)

def test_alarm_sync():
    """Test sending a synchronous alarm message to Discord."""
    # Create a fresh alarm instance to ensure it picks up the current environment
    alarm = Alarm(discord_url=settings.discord_webhook_url)
    
    if not alarm.discord_webhook_url:
        pytest.skip("No Discord Webhook URL configured")
        
    try:
        alarm.send_discord_sync(
            "🔔 **pytest Alarm Test (Sync)**\nThis is a test message from pytest.",
            username="Pytest Bot"
        )
    except Exception as e:
        pytest.fail(f"Sync alarm sending failed: {e}")

@pytest.mark.asyncio
async def test_alarm_async():
    """Test sending an asynchronous alarm message to Discord."""
    alarm = Alarm(discord_url=settings.discord_webhook_url)
    
    if not alarm.discord_webhook_url:
        pytest.skip("No Discord Webhook URL configured")
        
    try:
        await alarm.send_discord_async(
            "🚀 **pytest Alarm Test (Async)**\nThis is a test message from pytest.",
            username="Pytest Bot"
        )
    except Exception as e:
        pytest.fail(f"Async alarm sending failed: {e}")
