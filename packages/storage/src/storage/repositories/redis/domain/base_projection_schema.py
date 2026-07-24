from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

RedisValueNormalizer = Callable[[Any], str]


@dataclass(frozen=True)
class RedisProjectionField:
    """
    Redis projection에 저장할 단일 Hash field 정의.

    name:
        Redis Hash에 저장되는 field 이름.

    attr:
        값을 읽어올 Pydantic 모델의 실제 필드명.
        import 시 model_fields에 존재하는지 검증해서 오타를 초기에 잡는다.

    required:
        값이 없으면 projection 생성을 실패시켜야 하는 필드.

    purpose:
        필드의 역할 설명. describe() 출력과 문서화에 사용된다.

    always_store:
        값이 None이어도 빈 문자열로 Redis Hash에 저장해야 하는 필드.

    normalizer:
        모델에서 읽은 값을 Redis Hash에 저장할 문자열로 변환하는 함수.
        지정하지 않으면 기본 serialize_redis_value()를 사용한다.
    """

    name: str
    attr: str
    required: bool = False
    purpose: str = "payload"
    always_store: bool = False
    normalizer: RedisValueNormalizer | None = None

    def normalize(self, value: Any) -> str:
        if value is None:
            return ""
        if self.normalizer:
            return self.normalizer(value)
        return serialize_redis_value(value)


class BaseRedisProjection:
    """
    Redis projection schema 공통 동작.

    하위 클래스는 MODEL, FIELD_DEFINITIONS, PROJECTION_NAME만 정의하고
    도메인별 생성 메소드에서 _from_source()를 호출한다.
    """

    MODEL: ClassVar[type[Any]]
    FIELD_DEFINITIONS: ClassVar[tuple[RedisProjectionField, ...]]
    PROJECTION_NAME: ClassVar[str] = "Redis projection"
    _SCHEMA_VALIDATED: ClassVar[bool] = False

    def __init__(self, fields: dict[str, str]) -> None:
        self._fields = fields

    @classmethod
    def validate_schema_once(cls) -> None:
        if cls._SCHEMA_VALIDATED:
            return

        seen_names: set[str] = set()

        for field in cls.FIELD_DEFINITIONS:
            if field.name in seen_names:
                raise ValueError(f"Duplicate Redis projection field: {field.name}")
            seen_names.add(field.name)

            if field.attr not in cls.MODEL.model_fields:
                raise AttributeError(
                    f"{cls.PROJECTION_NAME} field references unknown "
                    f"{cls.MODEL.__name__} attr: "
                    f"name={field.name}, attr={field.attr}"
                )

        cls._SCHEMA_VALIDATED = True

    @classmethod
    def _from_source(cls, source: Any) -> "BaseRedisProjection":
        fields: dict[str, str] = {}

        for field in cls.FIELD_DEFINITIONS:
            value = cls._get_value(source, field.attr)

            if value is None:
                if field.required:
                    raise ValueError(
                        f"{cls.PROJECTION_NAME} required field is None: "
                        f"{field.name}"
                    )
                if field.always_store:
                    fields[field.name] = ""
                continue

            fields[field.name] = field.normalize(value)

        cls._validate_required_fields(fields)

        return cls(fields)

    @staticmethod
    def _get_value(source: Any, attr: str) -> Any:
        if isinstance(source, dict):
            return source.get(attr)

        return getattr(source, attr)

    @classmethod
    def _validate_required_fields(cls, fields: dict[str, str]) -> None:
        required_fields = frozenset(
            field.name for field in cls.FIELD_DEFINITIONS if field.required
        )
        missing = sorted(
            name for name in required_fields
            if name not in fields or fields[name] == ""
        )

        if missing:
            raise ValueError(
                f"{cls.PROJECTION_NAME} required fields missing: "
                + ", ".join(missing)
            )

    @classmethod
    def describe(cls) -> list[dict[str, Any]]:
        return [
            {
                "field": field.name,
                "attr": field.attr,
                "required": field.required,
                "purpose": field.purpose,
                "always_store": field.always_store,
            }
            for field in cls.FIELD_DEFINITIONS
        ]

    def to_hash(self) -> dict[str, str]:
        return dict(self._fields)


def normalize_int_string(value: Any) -> str:
    return str(int(value))


def normalize_upper_string(value: Any) -> str:
    return serialize_redis_value(value).upper()


def serialize_redis_value(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, Enum):
        return str(value.value)

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)

    return str(value)
