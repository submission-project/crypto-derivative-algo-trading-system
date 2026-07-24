class BinanceOrderState:
    new = "NEW"
    partially_filled = "PARTIALLY_FILLED"
    filled = "FILLED"
    canceled = "CANCELED"
    cancelled = "CANCELLED"
    expired = "EXPIRED"
    expired_in_match = "EXPIRED_IN_MATCH"
    rejected = "REJECTED"

# DOCS
# https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Algo-Order-Update
class BinanceConditionalOrderState:
    new = "NEW" # 이 상태는 조건부 주문이 Algo Service에 성공적으로 삽입되었지만 아직 트리거되지 않았음을 나타냄.
    triggering = "TRIGGERING" # 이 상태는 주문이 트리거 조건을 충족했으며 일치하는 엔진으로 전달되었음을 의미함.
    triggered = "TRIGGERED" # 이 상태는 주문이 매칭 엔진에 성공적으로 배치되었다는 의미함.
    finished = "FINISHED" # 이 상태는 트리거된 조건부 주문이 매칭 엔진에서 채워졌거나 취소했음을 나타냄.
    canceled = "CANCELED" # 이 상태는 조건부 주문이 취소되었음을 나타냄.
    expired = "EXPIRED" # 이 상태는 조건부 주문이 시스템에 의해 취소되었음을 나타냄. Ex)사용자가 GTE_GTC Time-In-Force 조건부 주문을 입력한 뒤 해당 심볼의 모든 포지션을 닫아 시스템 주도로 조건부 주문을 취소하는 경우가 있음.
    rejected = "REJECTED" # 이 상태는 조건부 주문이 매칭 엔진에 의해 거부되었음을 의미함.


BINANCE_EXCHANGE_CONDITIONAL_ORDER_UNKNOWN_STATUS = "LOCAL_UNKNOWN" # 현재 바이낸스 응답값 상에서 UNKNOWN 값은 따로 전달하지 않기 때문에, 로컬로 구분하기 위해 PREFIX로 LOCAL 붙임