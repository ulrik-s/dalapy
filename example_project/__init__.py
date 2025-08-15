"""Example project using dalapy's models."""

from .models import User, Product
from .repositories import Users, Products

__all__ = ["User", "Product", "Users", "Products"]
