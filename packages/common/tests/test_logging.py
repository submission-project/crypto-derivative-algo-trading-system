import logging
from unittest.mock import MagicMock

from common.logging import (
    ERROR_CATEGORY_KEY,
    LogErrorCategory,
    LogWarningCategory,
    WARNING_CATEGORY_KEY,
    log_error,
    log_infra_error,
    log_infra_warning,
    log_service_error,
    log_service_warning,
    log_system_error,
    log_system_warning,
    log_warning,
    setup_logger,
)


def test_setup_logger():
    logger = setup_logger("test_logger", logging.DEBUG)
    assert logger.name == "test_logger"
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 3

    # Avoid duplicate handlers
    logger2 = setup_logger("test_logger")
    assert len(logger2.handlers) == 3


def test_categorized_helpers_set_extra_category() -> None:
    lg = MagicMock(spec_set=logging.Logger)

    log_service_error(lg, "svc")
    assert (
        lg.error.call_args.kwargs["extra"][ERROR_CATEGORY_KEY]
        == LogErrorCategory.SERVICE.value
    )

    log_system_error(lg, "sys")
    assert (
        lg.error.call_args.kwargs["extra"][ERROR_CATEGORY_KEY]
        == LogErrorCategory.SYSTEM.value
    )

    log_infra_error(lg, "pg")
    assert (
        lg.error.call_args.kwargs["extra"][ERROR_CATEGORY_KEY]
        == LogErrorCategory.INFRA.value
    )

    log_error(lg, LogErrorCategory.SERVICE, "via log_error")
    assert (
        lg.error.call_args.kwargs["extra"][ERROR_CATEGORY_KEY]
        == LogErrorCategory.SERVICE.value
    )


def test_categorized_helpers_merge_existing_extra() -> None:
    lg = MagicMock(spec_set=logging.Logger)
    log_system_error(lg, "bad", extra={"order_id": "O-1"})
    ex = lg.error.call_args.kwargs["extra"]
    assert ex["order_id"] == "O-1"
    assert ex[ERROR_CATEGORY_KEY] == LogErrorCategory.SYSTEM.value


def test_warning_helpers_set_extra_category() -> None:
    lg = MagicMock(spec_set=logging.Logger)

    log_service_warning(lg, "svc")
    assert (
        lg.warning.call_args.kwargs["extra"][WARNING_CATEGORY_KEY]
        == LogWarningCategory.SERVICE.value
    )

    log_system_warning(lg, "sys")
    assert (
        lg.warning.call_args.kwargs["extra"][WARNING_CATEGORY_KEY]
        == LogWarningCategory.SYSTEM.value
    )

    log_infra_warning(lg, "slow")
    assert (
        lg.warning.call_args.kwargs["extra"][WARNING_CATEGORY_KEY]
        == LogWarningCategory.INFRA.value
    )

    log_warning(lg, LogWarningCategory.SERVICE, "via log_warning")
    assert (
        lg.warning.call_args.kwargs["extra"][WARNING_CATEGORY_KEY]
        == LogWarningCategory.SERVICE.value
    )


def test_warning_helpers_merge_existing_extra() -> None:
    lg = MagicMock(spec_set=logging.Logger)
    log_system_warning(lg, "retry", extra={"attempt": 2})
    ex = lg.warning.call_args.kwargs["extra"]
    assert ex["attempt"] == 2
    assert ex[WARNING_CATEGORY_KEY] == LogWarningCategory.SYSTEM.value
