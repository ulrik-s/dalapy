"""Domain models for the example project."""

from __future__ import annotations

from typing import Optional, List

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class CSModule:
    id: int
    sku: str
    price: float = 0.0
    currency: str = "SEK"
    version: Optional[str] = None
    tag: Optional[str] = None


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class System:
    id: int
    name: str
    csmodule_ids: List[int]


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class CSModuleGroup:
    id: int
    tag: str
    path: str


__all__ = [
    "CSModule",
    "CSModuleGroup",
    "System"
]
