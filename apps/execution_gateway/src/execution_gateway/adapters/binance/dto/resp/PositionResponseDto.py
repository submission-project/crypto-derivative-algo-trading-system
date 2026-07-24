"""
Binance USD-M Futures Position & Symbol Config Response DTOs.

Binance Docs:
  - GET /fapi/v3/positionRisk    (Position Information V3)
  - GET /fapi/v1/symbolConfig    (Symbol Configuration)
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class PositionRiskRespDto:
    """
    포지션 정보 응답 DTO.

    GET /fapi/v3/positionRisk

    Response Example:
    {
        "symbol": "ADAUSDT",
        "positionSide": "BOTH",
        "positionAmt": "0",
        "entryPrice": "0.0",
        "breakEvenPrice": "0.0",
        "markPrice": "0.35925000",
        "unRealizedProfit": "0.00000000",
        "liquidationPrice": "0",
        "isolatedMargin": "0.00000000",
        "notional": "0",
        "marginAsset": "USDT",
        "isolatedWallet": "0.00000000",
        "initialMargin": "0",
        "maintMargin": "0",
        "updateTime": 0
    }
    """
    symbol: Optional[str]
    positionSide: Optional[str]
    positionAmt: Optional[str]
    entryPrice: Optional[str]
    breakEvenPrice: Optional[str]
    markPrice: Optional[str]
    unRealizedProfit: Optional[str]
    liquidationPrice: Optional[str]
    isolatedMargin: Optional[str]
    notional: Optional[str]
    marginAsset: Optional[str]
    isolatedWallet: Optional[str]
    initialMargin: Optional[str]
    maintMargin: Optional[str]
    updateTime: Optional[int]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "PositionRiskRespDto":
        return cls(
            symbol=row.get("symbol"),
            positionSide=row.get("positionSide"),
            positionAmt=row.get("positionAmt"),
            entryPrice=row.get("entryPrice"),
            breakEvenPrice=row.get("breakEvenPrice"),
            markPrice=row.get("markPrice"),
            unRealizedProfit=row.get("unRealizedProfit"),
            liquidationPrice=row.get("liquidationPrice"),
            isolatedMargin=row.get("isolatedMargin"),
            notional=row.get("notional"),
            marginAsset=row.get("marginAsset"),
            isolatedWallet=row.get("isolatedWallet"),
            initialMargin=row.get("initialMargin"),
            maintMargin=row.get("maintMargin"),
            updateTime=row.get("updateTime"),
            raw=row,
        )


@dataclass(frozen=True)
class SymbolConfigRespDto:
    """
    심볼 설정 응답 DTO.

    GET /fapi/v1/symbolConfig

    Response Example:
    {
        "symbol": "BTCUSDT",
        "marginType": "CROSSED",
        "isAutoAddMargin": false,
        "leverage": 20,
        "maxNotionalValue": "1000000"
    }
    """
    symbol: str
    marginType: str
    isAutoAddMargin: bool
    leverage: int
    maxNotionalValue: str
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "SymbolConfigRespDto":
        return cls(
            symbol=row.get("symbol"),
            marginType=row.get("marginType"),
            isAutoAddMargin=row.get("isAutoAddMargin"),
            leverage=row.get("leverage"),
            maxNotionalValue=row.get("maxNotionalValue"),
            raw=row,
        )
