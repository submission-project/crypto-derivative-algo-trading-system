"""
Binance USDⓈ-M Futures REST API 전용 HMAC-SHA256 서명 유틸리티.

REST signed endpoint 규칙:
  - 파라미터를 urlencode()로 그대로 직렬화 (정렬 없음)
  - timestamp (ms) 필수
  - recvWindow 권장 (기본값 5000ms, 최대 60000ms)
  - HMAC-SHA256(query_string, api_secret) → hexdigest

WebSocket Trade API 서명은 binance_ws_auth.py를 사용.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any
from urllib.parse import urlencode

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from common.time import current_time_ms

DEFAULT_RECV_WINDOW = 5000  # ms, Binance 기본값 (최대 60000), Binance가 요청을 받아들일 시간 허용 범위

# BINANCE_API_KEY=...
# BINANCE_API_SECRET=...
def sign_rest_hmac(params: dict[str, Any], secret: str) -> dict[str, Any]:
    """
    REST signed endpoint용 HMAC-SHA256 서명.

    - timestamp 자동 주입 (없을 경우)
    - recvWindow 기본값 주입 (없을 경우)
    - urlencode 후 HMAC-SHA256 서명
    - signature를 params에 추가하여 반환

    Args:
        params: API 요청 파라미터
        secret: Binance API Secret

    Returns:
        timestamp, recvWindow, signature가 포함된 새 dict
    """
    signed = dict(params)

    if "timestamp" not in signed:
        signed["timestamp"] = current_time_ms()

    if "recvWindow" not in signed:
        signed["recvWindow"] = DEFAULT_RECV_WINDOW

    query_string = urlencode(signed)

    signature = hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    signed["signature"] = signature
    return signed


# BINANCE_ED25519_KEY_PEM=...
def sign_rest_ed25519(params: dict[str, Any], private_key_pem: str) -> dict[str, Any]:
    """
    REST signed endpoint용 Ed25519 서명.
    (Self-generated API Key 등 HMAC secret이 없는 경우 사용)

    - timestamp, recvWindow 주입
    - payload: urlencode 된 문자열 (WebSocket처럼 정렬하지 않음)
    - Ed25519 서명 후 base64(ASCII) 인코딩

    Args:
        params: API 요청 파라미터
        private_key_pem: Ed25519 Private Key PEM 문자열

    Returns:
        timestamp, recvWindow, signature가 포함된 새 dict
    """
    signed = dict(params)

    if "timestamp" not in signed:
        signed["timestamp"] = current_time_ms()

    if "recvWindow" not in signed:
        signed["recvWindow"] = DEFAULT_RECV_WINDOW

    query_string = urlencode(signed)

    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )

    if not isinstance(private_key, Ed25519PrivateKey):
        raise ValueError("Provided PEM is not an Ed25519 private key")

    signature = base64.b64encode(
        private_key.sign(query_string.encode("ASCII"))
    ).decode("ASCII")

    signed["signature"] = signature
    return signed


def create_rest_headers(api_key: str) -> dict[str, str]:
    """
    Binance REST API 인증 헤더 생성.

    Note: params는 query string으로 전송하므로 Content-Type은 불필요.
    """
    return {
        "X-MBX-APIKEY": api_key,
    }
