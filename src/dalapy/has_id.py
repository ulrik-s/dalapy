"""Shared protocol for entities with an integer ``id`` attribute."""

from typing import Protocol


class HasId(Protocol):
    """Protocol requiring an ``id`` attribute."""

    id: int
