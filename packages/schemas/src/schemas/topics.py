from .market import Exchange, MarketType


def _topic(exchange: Exchange, market_type: MarketType, data_type: str) -> str:
    """
    Build a topic name.

    Examples:
        market.trades.binance.perp
        market.depth.okx.spot
    """
    return f"market.{data_type}.{exchange.value}.{market_type.value}"


class TopicNames:
    """Pre-built topic name constants for common combinations."""

    # Binance Spot
    TRADES_BINANCE_SPOT = _topic(Exchange.BINANCE, MarketType.SPOT, "trades")
    TICKER_BINANCE_SPOT = _topic(Exchange.BINANCE, MarketType.SPOT, "ticker")
    DEPTH_BINANCE_SPOT = _topic(Exchange.BINANCE, MarketType.SPOT, "depth")

    # Binance Perp (USDT-M) — Legacy single topic
    TRADES_BINANCE_PERP = _topic(Exchange.BINANCE, MarketType.PERP, "trades")
    TICKER_BINANCE_PERP = _topic(Exchange.BINANCE, MarketType.PERP, "ticker")
    DEPTH_BINANCE_PERP = _topic(Exchange.BINANCE, MarketType.PERP, "depth")

    # Binance Perp (USDT-M) — 3-topic architecture
    RAW_TRADE_EVENTS_BINANCE_PERP    = _topic(Exchange.BINANCE, MarketType.PERP, "raw_trade_events")
    AGG_TRADE_EVENTS_BINANCE_PERP    = _topic(Exchange.BINANCE, MarketType.PERP, "agg_trade_events")
    CANONICAL_TRADES_BINANCE_PERP    = _topic(Exchange.BINANCE, MarketType.PERP, "canonical_trades")


    # OKX Spot
    TRADES_OKX_SPOT = _topic(Exchange.OKX, MarketType.SPOT, "trades")
    TICKER_OKX_SPOT = _topic(Exchange.OKX, MarketType.SPOT, "ticker")
    DEPTH_OKX_SPOT = _topic(Exchange.OKX, MarketType.SPOT, "depth")

    # OKX Perp
    TRADES_OKX_PERP = _topic(Exchange.OKX, MarketType.PERP, "trades")
    TICKER_OKX_PERP = _topic(Exchange.OKX, MarketType.PERP, "ticker")
    DEPTH_OKX_PERP = _topic(Exchange.OKX, MarketType.PERP, "depth")

    # Internal Streams
    ORDERBOOK_LOCAL = "internal.orderbook.local"

    # Signals
    SIGNALS = "strategy.signals"

    # Order Lifecycle
    ORDER_REQUESTS = "order.requests"                # API Server → Execution Gateway (주문 생성 요청)
    ORDER_STATE_UPDATES = "order.state_updates"      # Execution Gateway → API Server / Monitoring (상태 변경)
    ORDER_CANCEL_REQUESTS = "order.cancel_requests"  # API Server → Execution Gateway (취소 요청)
    EXECUTION_REPORTS = "execution.reports"           # Execution Gateway → Stream Processor (체결 이벤트)

    # Legacy (향후 자동 주문에서 사용 예정)
    ORDER_INTENTS = "strategy.order_intents"
