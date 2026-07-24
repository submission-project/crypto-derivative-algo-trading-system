from __future__ import annotations

import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any, Mapping

# Use a global executor to prevent blocking the event loop on error logs
_alarm_executor = ThreadPoolExecutor(max_workers=2)


class LogErrorCategory(str, Enum):
    """
    ERROR 로그 세분류 (Discord/Slack 알림 헤더·운영 분류용).

    - SERVICE: 비즈니스·도메인 (거래소 4xx, 주문 검증 등 복구·대응이 명확한 케이스)
    - SYSTEM: 애플리케이션 내부 결함 버그 가능성 높음 (예상 밖 상태, 처리 누락)
    - INFRA: 외부 인프라 (DB·Redis·네트워크 등 연결·타임아웃)
    """

    SERVICE = "service"
    SYSTEM = "system"
    INFRA = "infra"


# WARNING도 동일한 계층 구분을 쓴다 (별도 Enum을 두지 않음).
LogWarningCategory = LogErrorCategory


ERROR_CATEGORY_KEY = "error_category"
WARNING_CATEGORY_KEY = "warning_category"


def _merge_error_extra(
    extra: Mapping[str, Any] | None,
    category: LogErrorCategory,
) -> dict[str, Any]:
    base = dict(extra) if extra else {}
    base[ERROR_CATEGORY_KEY] = category.value
    return base


def _merge_warning_extra(
    extra: Mapping[str, Any] | None,
    category: LogWarningCategory,
) -> dict[str, Any]:
    base = dict(extra) if extra else {}
    base[WARNING_CATEGORY_KEY] = category.value
    return base


def _alarm_error_banner(record: logging.LogRecord) -> tuple[str, str]:
    """Discord/Slack 한 줄 헤더 (ERROR): (emoji, [TAG])."""
    raw = getattr(record, ERROR_CATEGORY_KEY, None)
    if raw == LogErrorCategory.SYSTEM.value:
        return "🔥", "[SYSTEM ERROR]"
    if raw == LogErrorCategory.SERVICE.value:
        return "⚠️", "[SERVICE ERROR]"
    if raw == LogErrorCategory.INFRA.value:
        return "🔌", "[INFRA ERROR]"
    return "🚨", "[ERROR]"


def _alarm_warning_banner(record: logging.LogRecord) -> tuple[str, str]:
    """Discord/Slack 한 줄 헤더 (WARNING): (emoji, [TAG])."""
    raw = getattr(record, WARNING_CATEGORY_KEY, None)
    if raw == LogWarningCategory.SYSTEM.value:
        return "🔥", "[SYSTEM WARNING]"
    if raw == LogWarningCategory.SERVICE.value:
        return "⚠️", "[SERVICE WARNING]"
    if raw == LogWarningCategory.INFRA.value:
        return "🔌", "[INFRA WARNING]"
    return "⚠️", "[WARNING]"


def log_service_error(
    logger: logging.Logger,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    logger.error(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_error_extra(extra, LogErrorCategory.SERVICE),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


def log_system_error(
    logger: logging.Logger,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    logger.error(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_error_extra(extra, LogErrorCategory.SYSTEM),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


def log_infra_error(
    logger: logging.Logger,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    logger.error(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_error_extra(extra, LogErrorCategory.INFRA),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


def log_error(
    logger: logging.Logger,
    category: LogErrorCategory,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    """통합 진입점: category만 바꿔 같은 시그니처로 기록."""
    logger.error(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_error_extra(extra, category),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


def log_service_warning(
    logger: logging.Logger,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    logger.warning(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_warning_extra(extra, LogWarningCategory.SERVICE),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


def log_system_warning(
    logger: logging.Logger,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    logger.warning(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_warning_extra(extra, LogWarningCategory.SYSTEM),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


def log_infra_warning(
    logger: logging.Logger,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    logger.warning(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_warning_extra(extra, LogWarningCategory.INFRA),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


def log_warning(
    logger: logging.Logger,
    category: LogWarningCategory,
    msg: str,
    *args: Any,
    exc_info: Any = False,
    extra: Mapping[str, Any] | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
) -> None:
    """통합 진입점: WARNING + 카테고리."""
    logger.warning(
        msg,
        *args,
        exc_info=exc_info,
        extra=_merge_warning_extra(extra, category),
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )


class AlarmLogHandler(logging.Handler):
    """
    ERROR / CRITICAL 로그 가로채서 Alarm(Discord/Slack 등) 비동기 전송.

    `extra=dict(error_category=...)` 또는 `log_*_error()` 헬퍼로 카테고리를 넣으면
    알림 제목에 [SERVICE ERROR] / [SYSTEM ERROR] / [INFRA ERROR] 로 구분된다.
    미지정 시 기존과 동일하게 [ERROR].
    """

    def __init__(self):
        super().__init__()
        self.setLevel(logging.ERROR)

    def emit(self, record):
        try:
            from common.alarm import alarm

            msg = self.format(record)
            emoji, tag = _alarm_error_banner(record)

            title = f"{emoji} **{tag} {record.name}**"

            if alarm.discord_webhook_url:
                _alarm_executor.submit(
                    alarm.send_discord_sync,
                    f"{title}\n```\n{msg}\n```",
                    "Takora Logger",
                )
            if alarm.slack_webhook_url:
                slack_title = emoji + " " + tag + " " + record.name
                _alarm_executor.submit(
                    alarm.send_slack_sync,
                    f"{slack_title}\n```\n{msg}\n```",
                    "Takora Logger",
                )
        except Exception:
            self.handleError(record)


class AlarmWarningLogHandler(logging.Handler):
    """
    WARNING 중 `log_*_warning()` / `extra[warning_category]` 가 붙은 레코드만 Alarm 전송.

    일반 `logger.warning()` 은 스팸 방지를 위해 웹훅으로 보내지 않는다.
    """

    def __init__(self):
        super().__init__()
        self.setLevel(logging.WARNING)

    def emit(self, record):
        if record.levelno != logging.WARNING:
            return
        if getattr(record, WARNING_CATEGORY_KEY, None) is None:
            return
        try:
            from common.alarm import alarm

            msg = self.format(record)
            emoji, tag = _alarm_warning_banner(record)

            title = f"{emoji} **{tag} {record.name}**"

            if alarm.discord_webhook_url:
                _alarm_executor.submit(
                    alarm.send_discord_sync,
                    f"{title}\n```\n{msg}\n```",
                    "Takora Logger",
                )
            if alarm.slack_webhook_url:
                slack_title = emoji + " " + tag + " " + record.name
                _alarm_executor.submit(
                    alarm.send_slack_sync,
                    f"{slack_title}\n```\n{msg}\n```",
                    "Takora Logger",
                )
        except Exception:
            self.handleError(record)


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    표준화된 logger 설정.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03dZ - %(levelname)s - %(name)s - %(funcName)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        alarm_handler = AlarmLogHandler()
        alarm_handler.setFormatter(formatter)
        logger.addHandler(alarm_handler)

        warn_alarm_handler = AlarmWarningLogHandler()
        warn_alarm_handler.setFormatter(formatter)
        logger.addHandler(warn_alarm_handler)

    return logger
