"""Domain models for the example project."""

from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class User:
    id: int
    name: str
    spend: float = 0.0
    email: Optional[str] = None


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class Product:
    id: int
    sku: str
    price: float = 0.0
    currency: str = "SEK"


__all__ = ["User", "Product"]
