"""
Binance WebSocket Trade API 전용 서명 유틸리티.

WebSocket signed request 규칙 (REST와 다름!):
  - params에 apiKey를 직접 포함
  - timestamp (ms) 필수
  - signature는 제외한 모든 params를 이름 기준 알파벳순 정렬 후 "&key=value" 형식으로 직렬화
  - Ed25519 서명 → base64 인코딩 (ASCII)
  - HMAC-SHA256도 지원하지만, 이 프로젝트는 Ed25519 우선

REST 서명은 binance_rest_auth.py를 사용.

참고:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info
"""

import base64

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from common.time import current_time_ms

import hashlib
import hmac as _hmac


def sign_ws_ed25519(
    params: dict,
    api_key: str,
    private_key_pem: str,
) -> dict:
    """
    WebSocket Trade API용 Ed25519 서명.

    REST와의 차이:
      1. params에 apiKey를 포함시킴
      2. signature 제외 모든 params를 이름 알파벳순으로 정렬한 후 payload 생성
      3. Ed25519 서명 → base64(ASCII)

    Args:
        params: WS request params (id 제외, method 제외)
        api_key: Binance API Key
        private_key_pem: Ed25519 PEM 형식 private key 문자열

    Returns:
        apiKey, timestamp, signature가 포함된 새 dict
    """
    signed = dict(params)

    # WS Trade API는 apiKey를 params 안에 포함
    signed["apiKey"] = api_key

    if "timestamp" not in signed:
        signed["timestamp"] = current_time_ms()

    # Binance WS API: signature 제외 params를 이름순 정렬하여 payload 생성
    payload = "&".join(
        f"{key}={value}"
        for key, value in sorted(signed.items())
        if key != "signature"
    )

    # Ed25519 서명
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )

    if not isinstance(private_key, Ed25519PrivateKey):
        raise ValueError("Provided PEM is not an Ed25519 private key")

    signature = base64.b64encode(
        private_key.sign(payload.encode("ASCII"))
    ).decode("ASCII")

    signed["signature"] = signature
    return signed


def sign_ws_hmac(
    params: dict,
    api_key: str,
    secret: str,
) -> dict:
    """
    WebSocket Trade API용 HMAC-SHA256 서명 (Ed25519 대안).

    Ed25519 키가 없을 경우에만 사용.
    REST와 동일하게 HMAC-SHA256을 사용하지만,
    payload 구성 규칙은 WS 기준 (알파벳순 정렬)을 따름.
    """

    signed = dict(params)
    signed["apiKey"] = api_key

    if "timestamp" not in signed:
        signed["timestamp"] = current_time_ms()

    payload = "&".join(
        f"{key}={value}"
        for key, value in sorted(signed.items())
        if key != "signature"
    )

    signature = _hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    signed["signature"] = signature
    return signed
