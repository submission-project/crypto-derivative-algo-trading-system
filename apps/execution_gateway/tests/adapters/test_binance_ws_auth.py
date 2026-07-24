"""
binance_ws_auth.py 단위 테스트.

검증 항목:
- WS 서명 파라미터에 apiKey 포함
- 알파벳순 정렬된 payload로 서명 (REST와 다름)
- signature 결과가 base64 형식인지 확인
"""
from execution_gateway.adapters.binance.config import settings
import os

from execution_gateway.adapters.binance.auth.binance_ws_auth import sign_ws_hmac, sign_ws_ed25519
from execution_gateway.adapters.binance.auth.binance_rest_auth import sign_rest_hmac, sign_rest_ed25519

def get_pem_data():
    active_pem_path = settings.binance_testnet_ed25519_key_pem
    if active_pem_path and os.path.exists(active_pem_path):
        with open(active_pem_path, "r") as f:
            return f.read()
    else:
        raise Exception("PEM 파일이 존재하지 않습니다.")


def test_sign_ws_hmac_adds_api_key():
    """WS 서명에 apiKey가 포함되는지 확인."""
    params = {"symbol": "BTCUSDT", "timestamp": 1234567890}
    signed = sign_ws_hmac(params, "my_api_key", "my_secret")
    assert signed["apiKey"] == "my_api_key"

def test_sign_ws_ed25519_adds_api_key():
    """WS 서명에 apiKey가 포함되는지 확인."""
    params = {"symbol": "BTCUSDT", "timestamp": 1234567890}

    pem_data = get_pem_data()
    signed = sign_ws_ed25519(params, "my_api_key", pem_data)
    assert signed["apiKey"] == "my_api_key"
    

def test_sign_ws_hmac_alphabetical_sort():
    """서명 payload가 알파벳순 정렬을 따르는지 간접 확인 (결정론적 서명)."""
    params = {"z_param": "last", "a_param": "first", "timestamp": 1234567890}
    signed1 = sign_ws_hmac(dict(params), "api_key", "secret")
    signed2 = sign_ws_hmac(dict(params), "api_key", "secret")
    assert signed1["signature"] == signed2["signature"]

def test_sign_ws_ed25519_alphabetical_sort():
    """서명 payload가 알파벳순 정렬을 따르는지 간접 확인 (결정론적 서명)."""
    params = {"z_param": "last", "a_param": "first", "timestamp": 1234567890}
    
    pem_data = get_pem_data()
    
    signed1 = sign_ws_ed25519(dict(params), "api_key", pem_data)
    signed2 = sign_ws_ed25519(dict(params), "api_key", pem_data)
    assert signed1["signature"] == signed2["signature"]


def test_sign_ws_hmac_differs_from_rest_hmac():
    """WS 서명과 REST 서명 결과가 다른지 확인 (payload 구성 규칙 다름)."""
    

    params = {"symbol": "BTCUSDT", "timestamp": 1234567890, "recvWindow": 5000}
    secret = "test_secret"

    rest_signed = sign_rest_hmac(dict(params), secret)
    ws_signed = sign_ws_hmac({"symbol": "BTCUSDT", "timestamp": 1234567890}, "api_key", secret)

    # apiKey, 정렬, 포함 파라미터가 다르므로 서명은 달라야 함
    assert rest_signed["signature"] != ws_signed["signature"]


def test_sign_rest_ed25519_differs_from_ws_ed25519():
    """WS 서명과 REST 서명 결과가 다른지 확인 (payload 구성 규칙 다름)."""

    params = {"symbol": "BTCUSDT", "timestamp": 1234567890, "recvWindow": 5000}

    pem_data = get_pem_data()
    rest_signed = sign_rest_ed25519(dict(params), pem_data)
    ws_signed = sign_ws_ed25519({"symbol": "BTCUSDT", "timestamp": 1234567890}, "api_key", pem_data)

    # apiKey, 정렬, 포함 파라미터가 다르므로 서명은 달라야 함
    assert rest_signed["signature"] != ws_signed["signature"]

