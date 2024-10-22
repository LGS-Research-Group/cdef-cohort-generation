from typing import Any, TypedDict


class RegisterKwargs(TypedDict, total=False):
    longitudinal: bool
    join_on: str | list[str]
    date_columns: list[str] | None
    register_name: str

KwargsType = Any
