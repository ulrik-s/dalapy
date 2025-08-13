"""User entity model."""

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass

from .has_id import HasId


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class User(HasId):
    id: int
    name: str
    spend: float = 0.0
