from dataclasses import dataclass
from typing import Any


# # [claim] : 제거 필요 => [complete]
# @dataclass(frozen=True, slots=True)
# class OrderUpdateEnvelope:
#     event_time: int | None
#     transaction_time: int | None
#     order: dict[str, Any]
#     raw: dict[str, Any]

# # [claim] : 제거 필요 => [complete]
# @dataclass(frozen=True, slots=True)
# class AccountUpdateEnvelope:
#     event_time: int | None
#     raw: dict[str, Any]

# @dataclass
# class AlgoUpdateEnvelope:
#     event_time: int
#     transaction_time: int | None
#     algo: dict[str, Any]
#     raw: dict[str, Any]