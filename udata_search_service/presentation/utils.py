from typing import Type, get_origin, Union, get_args


def is_list_type(type_: Type) -> bool:
    origin = get_origin(type_)
    if origin is list:
        return True
    if origin is Union:
        return any(is_list_type(t) for t in get_args(type_))
    return False
