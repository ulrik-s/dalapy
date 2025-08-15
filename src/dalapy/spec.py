from __future__ import annotations

from typing import Any, Callable, Optional, Tuple, TypeVar, Generic, Protocol

from pydantic import ConfigDict, TypeAdapter
from pydantic.dataclasses import dataclass as pyd_dataclass
from tinydb.table import Table

from .env import Env


class HasId(Protocol):
    id: int


T = TypeVar('T', bound=HasId)


@pyd_dataclass(config=ConfigDict(extra='ignore'))
class UniqueRule:
    field: str
    nocase: bool = False
    allow_none: bool = True


CustomValidator = Callable[[Env, Table, T, Optional[int]], Optional[str]]


@pyd_dataclass(config=ConfigDict(extra='ignore', arbitrary_types_allowed=True))
class RepoSpec(Generic[T]):
    table_name: str
    adapter: TypeAdapter  # TypeAdapter[T]
    unique: Tuple[UniqueRule, ...] = ()
    validator: Optional[CustomValidator] = None


def _norm(rule: UniqueRule, value: Any):
    if value is None:
        return None
    return str(value).lower() if rule.nocase else value


def validate(env: Env, tbl: Table, spec: RepoSpec[T], obj: T, *, exclude_id: Optional[int]) -> Optional[str]:
    for rule in spec.unique:
        val = _norm(rule, getattr(obj, rule.field, None))
        if val is None and rule.allow_none:
            continue
        for row in tbl:
            if exclude_id is not None and row.get('id') == exclude_id:
                continue
            if _norm(rule, row.get(rule.field)) == val:
                return f'unique_violation:{rule.field}'
    if spec.validator:
        err = spec.validator(env, tbl, obj, exclude_id)
        if err:
            return err
    return None
