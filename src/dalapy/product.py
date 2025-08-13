"""Product entity model."""

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass

from .has_id import HasId


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class Product(HasId):
    id: int
    sku: str
    price: float = 0.0
    currency: str = "SEK"
