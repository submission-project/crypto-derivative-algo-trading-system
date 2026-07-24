from schemas.binance_usds_futures import (
    BinanceUsdsFuturesExecutionType,
    parse_binance_usds_futures_execution_type,
)


def test_parse_trade() -> None:
    assert (
        parse_binance_usds_futures_execution_type("TRADE")
        is BinanceUsdsFuturesExecutionType.TRADE
    )


def test_parse_whitespace_trade() -> None:
    assert (
        parse_binance_usds_futures_execution_type("  TRADE ")
        is BinanceUsdsFuturesExecutionType.TRADE
    )


def test_parse_none_and_missing() -> None:
    assert parse_binance_usds_futures_execution_type(None) is None
    assert parse_binance_usds_futures_execution_type("") is None
    assert parse_binance_usds_futures_execution_type("   ") is None


def test_parse_member_instance() -> None:
    assert (
        parse_binance_usds_futures_execution_type(
            BinanceUsdsFuturesExecutionType.TRADE
        )
        is BinanceUsdsFuturesExecutionType.TRADE
    )


def test_parse_unknown() -> None:
    assert parse_binance_usds_futures_execution_type("FUTURE_TYPE") is None


def test_all_documented_execution_types_exist() -> None:
    documented = frozenset(
        {
            "NEW",
            "CANCELED",
            "CALCULATED",
            "EXPIRED",
            "TRADE",
            "AMENDMENT",
        }
    )
    assert {m.value for m in BinanceUsdsFuturesExecutionType} == documented
