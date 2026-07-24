from typing import Any, Callable

# def enum_value(value: Any) -> Any:
#     return value.value if hasattr(value, "value") else value

def enum_value(value: Any, cast_to: Callable[[Any], Any] | None = None) -> Any:
    value = value.value if hasattr(value, "value") else value
    if cast_to:
        value = cast_to(value)
    return value
