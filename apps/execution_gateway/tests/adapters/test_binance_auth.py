"""
binance_rest_auth.py 단위 테스트.

검증 항목:
- timestamp 자동 주입
- recvWindow 기본값 주입
- 서명 생성 (hexdigest, 64자)
- 헤더 생성
"""
from execution_gateway.adapters.binance.auth.binance_rest_auth import sign_rest_hmac, create_rest_headers


def test_sign_rest_hmac_adds_timestamp_and_recv_window():
    """timestamp와 recvWindow가 자동 주입되는지 확인."""
    secret = "test_secret"
    params = {"symbol": "BTCUSDT"}
    signed = sign_rest_hmac(params, secret)

    assert "timestamp" in signed
    assert isinstance(signed["timestamp"], int)
    assert "recvWindow" in signed
    assert signed["recvWindow"] == 5000
    assert "signature" in signed
    assert len(signed["signature"]) == 64  # HMAC-SHA256 hexdigest


def test_sign_rest_hmac_does_not_override_existing_timestamp():
    """이미 timestamp가 있으면 덮어쓰지 않는지 확인."""
    secret = "test_secret"
    params = {"symbol": "BTCUSDT", "timestamp": 1499827319559}
    signed = sign_rest_hmac(params, secret)
    assert signed["timestamp"] == 1499827319559


def test_sign_rest_hmac_does_not_override_recv_window():
    """이미 recvWindow가 있으면 덮어쓰지 않는지 확인."""
    secret = "test_secret"
    params = {"symbol": "BTCUSDT", "recvWindow": 3000}
    signed = sign_rest_hmac(params, secret)
    assert signed["recvWindow"] == 3000


def test_sign_rest_hmac_known_value():
    """알려진 값으로 서명이 결정론적인지 확인."""
    secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiC1xl5IhiI0w4z7MUVZpXv9G4Cg4eC0S1A"
    params = {
        "symbol": "LTCBTC",
        "side": "BUY",
        "type": "LIMIT",
        "timestamp": 1499827319559,
        "recvWindow": 5000,
    }
    signed = sign_rest_hmac(params, secret)
    # 같은 입력에 대해 항상 같은 서명이 나와야 함
    signed2 = sign_rest_hmac(params, secret)
    assert signed["signature"] == signed2["signature"]


def test_create_rest_headers():
    """X-MBX-APIKEY 헤더가 올바른지 확인."""
    headers = create_rest_headers("my_api_key")
    assert headers["X-MBX-APIKEY"] == "my_api_key"
    # Content-Type은 포함하지 않음 (query string 방식이므로)
    assert "Content-Type" not in headers
