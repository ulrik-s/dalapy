"""Collection metadata describing how entities are stored."""

from __future__ import annotations

from typing import Generic, Type, TypeVar

from pydantic import ConfigDict, TypeAdapter
from pydantic.dataclasses import dataclass as pyd_dataclass

from .has_id import HasId

T = TypeVar("T", bound=HasId)


@pyd_dataclass(config=ConfigDict(extra="ignore", arbitrary_types_allowed=True))
class Collection(Generic[T]):
    """Associates a model class with its storage name and adapter."""

    name: str
    adapter: TypeAdapter  # TypeAdapter[T]


def collection_for(cls: Type[T], name: str | None = None) -> Collection[T]:
    """Attach type info + collection name to an entity class."""
    return Collection(name=(name or f"{cls.__name__.lower()}s"), adapter=TypeAdapter(cls))
