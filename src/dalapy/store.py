"""Generic in-memory store with versioning."""

from __future__ import annotations

from dataclasses import field
from typing import Dict

from pydantic import ConfigDict, TypeAdapter
from pydantic.dataclasses import dataclass as pyd_dataclass

CURRENT_STORE_VERSION = 1


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class GenericStore:
    """Serialized representation of stored collections."""

    version: int = CURRENT_STORE_VERSION
    # one big map: collection -> { id -> raw-dict }
    data: Dict[str, Dict[int, dict]] = field(default_factory=dict)


STORE_ADAPTER = TypeAdapter(GenericStore)
