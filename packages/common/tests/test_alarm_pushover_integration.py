"""
Pushover 실전 전송 테스트.

.env에 PUSHOVER_USER_KEY·PUSHOVER_API_TOKEN이 모두 있을 때만 실행된다.
실행 예:

  ENV_FILE=.env.dev uv run pytest \\
    packages/common/tests/test_alarm_pushover_integration.py -v -s

외부 네트워크 호출이므로 integration 마커를 붙인다 (-m \"not integration\" 으로 제외 가능).
"""

import pytest

from common.alarm import Alarm
from common.config import settings


def _pushover_configured() -> bool:
    return bool(settings.pushover_user_key and settings.pushover_api_token)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _pushover_configured(),
        reason="PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN must both be set",
    ),
]


def test_pushover_live_sync_sends_message() -> None:
    alarm = Alarm()
    try:
        alarm.send_pushover_sync(
            "pytest Pushover 실전 테스트 (sync)\n기기에 알림이 오면 성공입니다.",
            title="Takora pytest",
        )
    except Exception as e:
        pytest.fail(f"Pushover sync send failed: {e}")


@pytest.mark.asyncio
async def test_pushover_live_async_sends_message() -> None:
    alarm = Alarm()
    try:
        await alarm.send_pushover_async(
            "pytest Pushover 실전 테스트 (async)\n기기에 알림이 오면 성공입니다.",
            title="Takora pytest",
        )
    except Exception as e:
        pytest.fail(f"Pushover async send failed: {e}")
