"""Repository factories for the example project models."""

from __future__ import annotations

from pydantic import TypeAdapter

from dalapy import UniqueRule, RepoSpec, repo_factory

from .models import User, Product


USER_SPEC = RepoSpec[User](
    table_name="users",
    adapter=TypeAdapter(User),
    unique=(UniqueRule("name", nocase=True),),
)

PRODUCT_SPEC = RepoSpec[Product](
    table_name="products",
    adapter=TypeAdapter(Product),
    unique=(UniqueRule("sku", nocase=False),),
)

Users = repo_factory(USER_SPEC)
Products = repo_factory(PRODUCT_SPEC)

__all__ = [
    "USER_SPEC",
    "PRODUCT_SPEC",
    "Users",
    "Products",
]
