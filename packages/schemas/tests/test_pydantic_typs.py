from typing import Optional, Union
from pydantic import BaseModel
import typing
import types

class Order(BaseModel):
    id: int
    name: str
    is_active: bool
    count: Optional[int] = None
    count2: int | None = None
    flag: bool | None = None

def get_fields():
    int_fields = set()
    bool_fields = set()
    
    def contains_type(ann, target_type):
        if ann is target_type:
            return True
        origin = typing.get_origin(ann)
        if origin is typing.Union or type(ann) is types.UnionType:
            args = typing.get_args(ann)
            return target_type in args
        return False

    for name, info in Order.model_fields.items():
        if contains_type(info.annotation, int):
            int_fields.add(name)
        elif contains_type(info.annotation, bool):
            bool_fields.add(name)
            
    print("INT_FIELDS:", int_fields)
    print("BOOL_FIELDS:", bool_fields)

get_fields()
