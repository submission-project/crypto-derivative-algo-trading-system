"""
Binance USD-M Futures Account Response DTOs.

Binance Docs:
  - GET /fapi/v3/account (Account Information V3)

DOCS: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V3
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class AccountAssetRespDto:
    """
    계정 자산 정보 (account.assets[] 각 항목).

    Response Example:
    {
        "asset": "USDT",
        "walletBalance": "23.72469206",
        "unrealizedProfit": "0.00000000",
        "marginBalance": "23.72469206",
        "maintMargin": "0.00000000",
        "initialMargin": "0.00000000",
        "positionInitialMargin": "0.00000000",
        "openOrderInitialMargin": "0.00000000",
        "crossWalletBalance": "23.72469206",
        "crossUnPnl": "0.00000000",
        "availableBalance": "23.72469206",
        "maxWithdrawAmount": "23.72469206",
        "updateTime": 1625474304765
    }
    """
    asset: Optional[str]
    walletBalance: Optional[str]
    unrealizedProfit: Optional[str]
    marginBalance: Optional[str]
    maintMargin: Optional[str]
    initialMargin: Optional[str]
    positionInitialMargin: Optional[str]
    openOrderInitialMargin: Optional[str]
    crossWalletBalance: Optional[str]
    crossUnPnl: Optional[str]
    availableBalance: Optional[str]
    maxWithdrawAmount: Optional[str]
    updateTime: Optional[int]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "AccountAssetRespDto":
        return cls(
            asset=row.get("asset"),
            walletBalance=row.get("walletBalance"),
            unrealizedProfit=row.get("unrealizedProfit"),
            marginBalance=row.get("marginBalance"),
            maintMargin=row.get("maintMargin"),
            initialMargin=row.get("initialMargin"),
            positionInitialMargin=row.get("positionInitialMargin"),
            openOrderInitialMargin=row.get("openOrderInitialMargin"),
            crossWalletBalance=row.get("crossWalletBalance"),
            crossUnPnl=row.get("crossUnPnl"),
            availableBalance=row.get("availableBalance"),
            maxWithdrawAmount=row.get("maxWithdrawAmount"),
            updateTime=row.get("updateTime"),
            raw=row,
        )


@dataclass(frozen=True)
class AccountPositionRespDto:
    """
    계정 포지션 정보 (account.positions[] 각 항목).

    Response Example:
    {
        "symbol": "BTCUSDT",
        "positionSide": "BOTH",
        "positionAmt": "1.000",
        "unrealizedProfit": "0.00000000",
        "isolatedMargin": "0.00000000",
        "notional": "0",
        "isolatedWallet": "0",
        "initialMargin": "0",
        "maintMargin": "0",
        "updateTime": 0
    }
    """
    symbol: Optional[str]
    positionSide: Optional[str]
    positionAmt: Optional[str]
    unrealizedProfit: Optional[str]
    isolatedMargin: Optional[str]
    notional: Optional[str]
    isolatedWallet: Optional[str]
    initialMargin: Optional[str]
    maintMargin: Optional[str]
    updateTime: Optional[int]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "AccountPositionRespDto":
        return cls(
            symbol=row.get("symbol"),
            positionSide=row.get("positionSide"),
            positionAmt=row.get("positionAmt"),
            unrealizedProfit=row.get("unrealizedProfit"),
            isolatedMargin=row.get("isolatedMargin"),
            notional=row.get("notional"),
            isolatedWallet=row.get("isolatedWallet"),
            initialMargin=row.get("initialMargin"),
            maintMargin=row.get("maintMargin"),
            updateTime=row.get("updateTime"),
            raw=row,
        )


@dataclass(frozen=True)
class AccountInfoRespDto:
    """
    계정 정보 응답 DTO.

    GET /fapi/v3/account

    Response Example (top-level):
    {
        "totalInitialMargin": "0.00000000",
        "totalMaintMargin": "0.00000000",
        "totalWalletBalance": "103.12345678",
        "totalUnrealizedProfit": "0.00000000",
        "totalMarginBalance": "103.12345678",
        "totalPositionInitialMargin": "0.00000000",
        "totalOpenOrderInitialMargin": "0.00000000",
        "totalCrossWalletBalance": "103.12345678",
        "totalCrossUnPnl": "0.00000000",
        "availableBalance": "103.12345678",
        "maxWithdrawAmount": "103.12345678",
        "assets": [...],
        "positions": [...]
    }
    """
    totalInitialMargin: Optional[str]
    totalMaintMargin: Optional[str]
    totalWalletBalance: Optional[str]
    totalUnrealizedProfit: Optional[str]
    totalMarginBalance: Optional[str]
    totalPositionInitialMargin: Optional[str]
    totalOpenOrderInitialMargin: Optional[str]
    totalCrossWalletBalance: Optional[str]
    totalCrossUnPnl: Optional[str]
    availableBalance: Optional[str]
    maxWithdrawAmount: Optional[str]
    assets: list[AccountAssetRespDto]
    positions: list[AccountPositionRespDto]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "AccountInfoRespDto":
        return cls(
            totalInitialMargin=row.get("totalInitialMargin"),
            totalMaintMargin=row.get("totalMaintMargin"),
            totalWalletBalance=row.get("totalWalletBalance"),
            totalUnrealizedProfit=row.get("totalUnrealizedProfit"),
            totalMarginBalance=row.get("totalMarginBalance"),
            totalPositionInitialMargin=row.get("totalPositionInitialMargin"),
            totalOpenOrderInitialMargin=row.get("totalOpenOrderInitialMargin"),
            totalCrossWalletBalance=row.get("totalCrossWalletBalance"),
            totalCrossUnPnl=row.get("totalCrossUnPnl"),
            availableBalance=row.get("availableBalance"),
            maxWithdrawAmount=row.get("maxWithdrawAmount"),
            assets=[
                AccountAssetRespDto.from_response(a)
                for a in row.get("assets", [])
            ],
            positions=[
                AccountPositionRespDto.from_response(p)
                for p in row.get("positions", [])
            ],
            raw=row,
        )
