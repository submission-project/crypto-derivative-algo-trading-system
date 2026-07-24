from schemas.order import RejectReason
from execution_gateway.exchange import ExchangeApiError, ExchangeErrorCategory


def _map_exchange_error_to_reason(e: ExchangeApiError) -> RejectReason:
    if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
        return RejectReason.UNKNOWN_EXECUTION

    if e.category in (
        ExchangeErrorCategory.RATE_LIMITED,
        ExchangeErrorCategory.IP_BANNED,
        ExchangeErrorCategory.WAF_BLOCKED,
        ExchangeErrorCategory.SYSTEM_THROTTLE,
    ):
        return RejectReason.RATE_LIMITED

    if e.category == ExchangeErrorCategory.INSUFFICIENT_BALANCE:
        return RejectReason.INSUFFICIENT_BALANCE

    if e.category in (
        ExchangeErrorCategory.INVALID_SYMBOL,
        ExchangeErrorCategory.INVALID_PARAMETER,
    ):
        return RejectReason.INVALID_SYMBOL

    return RejectReason.EXCHANGE_REJECTED
