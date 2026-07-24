import pytest
from common.env import get_env, is_dev, is_prod, ENV_KEY_ENVIRONMENT, ENV_DEV, ENV_PROD

import random

def test_get_env(monkeypatch):
    def randomize_case(text):
        # 각 문자에 대해 upper()와 lower() 중 하나를 무작위로 선택
        return ''.join(random.choice([char.upper(), char.lower()]) for char in text)

    monkeypatch.setenv(ENV_KEY_ENVIRONMENT, randomize_case(ENV_PROD))
    assert get_env() == ENV_PROD
    assert is_prod() is True
    assert is_dev() is False
    
    monkeypatch.setenv(ENV_KEY_ENVIRONMENT, randomize_case(ENV_DEV))
    assert get_env() == ENV_DEV
    assert is_prod() is False
    assert is_dev() is True
    
    monkeypatch.delenv(ENV_KEY_ENVIRONMENT, raising=False)
    assert get_env() == ENV_DEV
