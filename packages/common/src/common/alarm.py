import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import asyncio
from typing import Optional
from common.config import settings

logger = logging.getLogger(__name__)

# Discord webhook content 필드 최대 길이 초과 시 400 → 알림 미전달
_DISCORD_CONTENT_MAX = 2000

_PUSHOVER_MESSAGES_URL = "https://api.pushover.net/1/messages.json"
_PUSHOVER_MESSAGE_MAX = 1024
_PUSHOVER_TITLE_MAX = 250
# priority=2(긴급)일 때는 retry·expire가 모두 필수. expire 없으면 API가 거부해 알림이 오지 않음.
_PUSHOVER_EMERGENCY_RETRY_SEC = 30
_PUSHOVER_EMERGENCY_EXPIRE_SEC = 3600  # 최대 86400, 이내에서 재시도 후 포기


class Alarm:
    """
    Independent Alarm class for sending notifications to Discord, Slack, or Pushover.
    Can be used across the application for events, signals, or errors.
    """

    def __init__(
        self,
        discord_url: Optional[str] = None,
        slack_url: Optional[str] = None,
        pushover_user_key: Optional[str] = None,
        pushover_api_token: Optional[str] = None,
    ):
        self.discord_webhook_url = discord_url or settings.discord_webhook_url
        self.slack_webhook_url = slack_url or settings.slack_webhook_url
        self.pushover_user_key = pushover_user_key or settings.pushover_user_key
        self.pushover_api_token = pushover_api_token or settings.pushover_api_token

    @staticmethod
    def _truncate_discord_content(message: str) -> str:
        if len(message) <= _DISCORD_CONTENT_MAX:
            return message
        suffix = "\n...(truncated)"
        return message[: _DISCORD_CONTENT_MAX - len(suffix)] + suffix

    def _send_sync(self, url: str, payload: dict):
        if not url:
            return
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Takora-Alarm-Bot",
                },
            )
            # Use a short timeout so we don't block heavily on sync calls
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status not in (200, 204):
                    logger.error(
                        f"Failed to send alarm webhook: HTTP {response.status}"
                    )
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:800]
            logger.error(
                "Alarm webhook HTTP error: %s %s body=%s",
                e.code,
                getattr(e, "reason", ""),
                body,
            )
        except Exception as e:
            logger.error(f"Error sending alarm webhook: {e}")

    def _send_pushover_sync(self, message: str, title: Optional[str] = None) -> None:
        if not self.pushover_user_key or not self.pushover_api_token:
            return
        msg = message[:_PUSHOVER_MESSAGE_MAX]
        data: dict[str, str] = {
            "token": self.pushover_api_token,
            "user": self.pushover_user_key,
            "message": msg,
            # 긴급: Pushover는 priority=2일 때 retry·expire 필수 (미설정 시 전송 실패)
            "priority": "2",
            "retry": str(_PUSHOVER_EMERGENCY_RETRY_SEC),
            "expire": str(_PUSHOVER_EMERGENCY_EXPIRE_SEC),
            "sound": "siren",
        }
        if title:
            data["title"] = title[:_PUSHOVER_TITLE_MAX]
        body = urllib.parse.urlencode(data).encode("utf-8")
        try:
            req = urllib.request.Request(
                _PUSHOVER_MESSAGES_URL,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "Takora-Alarm-Bot",
                },
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                raw = response.read().decode("utf-8", errors="replace")
                if response.status != 200:
                    logger.error(
                        f"Pushover alarm failed: HTTP {response.status} body={raw[:500]}"
                    )
                    return
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    logger.error(f"Pushover alarm: invalid JSON response: {raw[:500]}")
                    return
                if parsed.get("status") != 1:
                    errs = parsed.get("errors")
                    logger.error(
                        "Pushover alarm rejected: %s errors=%s",
                        parsed,
                        errs,
                    )
        except Exception as e:
            logger.error(f"Error sending Pushover alarm: {e}")

    async def send_discord_async(self, message: str, username: str = "Takora Alarm"):
        """Send an async notification to Discord."""
        if not self.discord_webhook_url:
            logger.debug("Discord webhook URL not configured.")
            return

        message = self._truncate_discord_content(message)
        payload = {"content": message, "username": username}
        await asyncio.to_thread(self._send_sync, self.discord_webhook_url, payload)

    async def send_slack_async(self, message: str, username: str = "Takora Alarm"):
        """Send an async notification to Slack."""
        if not self.slack_webhook_url:
            logger.debug("Slack webhook URL not configured.")
            return

        payload = {"text": message, "username": username}
        await asyncio.to_thread(self._send_sync, self.slack_webhook_url, payload)

    async def send_pushover_async(
        self, message: str, title: str = "Takora Alarm"
    ) -> None:
        """Send an async notification via Pushover."""
        if not self.pushover_user_key or not self.pushover_api_token:
            logger.debug("Pushover user key or API token not configured.")
            return
        await asyncio.to_thread(self._send_pushover_sync, message, title)

    def send_discord_sync(self, message: str, username: str = "Takora Alarm"):
        """Send a sync notification to Discord."""
        if not self.discord_webhook_url:
            logger.debug("Discord webhook URL not configured.")
            return

        message = self._truncate_discord_content(message)
        payload = {"content": message, "username": username}
        self._send_sync(self.discord_webhook_url, payload)

    def send_slack_sync(self, message: str, username: str = "Takora Alarm"):
        """Send a sync notification to Slack."""
        if not self.slack_webhook_url:
            logger.debug("Slack webhook URL not configured.")
            return

        payload = {"text": message, "username": username}
        self._send_sync(self.slack_webhook_url, payload)

    def send_pushover_sync(self, message: str, title: str = "Takora Alarm") -> None:
        """Send a sync notification via Pushover."""
        if not self.pushover_user_key or not self.pushover_api_token:
            logger.debug("Pushover user key or API token not configured.")
            return
        self._send_pushover_sync(message, title)


# Create a default global instance that picks up config
alarm = Alarm()
