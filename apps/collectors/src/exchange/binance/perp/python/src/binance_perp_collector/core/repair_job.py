from dataclasses import dataclass, asdict

# RepairJob은 aggTrade 스트림에서 누락된 개별 체결 구간을 REST로 복원하기 위한 불변 작업 명세 객체
# 인스턴스 생성 후 필드 값을 변경 불가능(immutable) 하게 만듦.
@dataclass(frozen=True, slots=True)
class RepairJob:
    """aggTrade에서 발생한 f~l 범위를 REST로 복원하기 위한 작업 단위"""

    symbol: str
    from_trade_id: int
    to_trade_id: int
    source_agg_trade_id: int
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "RepairJob":
        return cls(**data)
