from __future__ import annotations

from schemas.market import Exchange, MarketType


def parse_enabled_execution_markets(
    raw: str | None,
) -> list[tuple[Exchange, MarketType]]:
    if not raw:
        return [(Exchange.BINANCE, MarketType.PERP)]

    markets: list[tuple[Exchange, MarketType]] = []

    for item in raw.split(","):
        value = item.strip()
        if not value:
            continue

        try:
            exchange_raw, market_type_raw = value.split(":", 1)
        except ValueError as e:
            raise ValueError(
                f"invalid execution market format: {value!r}. "
                f"expected EXCHANGE:MARKET_TYPE"
            ) from e

        markets.append(
            (
                Exchange(exchange_raw.strip().upper()),
                MarketType(market_type_raw.strip().upper()),
            )
        )

    return markets