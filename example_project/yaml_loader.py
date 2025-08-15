"""Utility functions to load YAML data into the example project repos."""

from __future__ import annotations

from pathlib import Path
from typing import List

import yaml
from returns.io import IOResult

from dalapy import Env
from .models import User, Product
from .repositories import Users, Products


def load_users_from_yaml(path: Path, env: Env) -> List[IOResult[User, str]]:
    """Read a YAML file of users and insert them into the database."""
    data = yaml.safe_load(path.read_text()) or []
    results: List[IOResult[User, str]] = []
    for item in data:
        user = User(**item)
        results.append(Users.create_flow(user)(env))
    return results


def load_products_from_yaml(path: Path, env: Env) -> List[IOResult[Product, str]]:
    """Read a YAML file of products and insert them into the database."""
    data = yaml.safe_load(path.read_text()) or []
    results: List[IOResult[Product, str]] = []
    for item in data:
        product = Product(**item)
        results.append(Products.create_flow(product)(env))
    return results


__all__ = ["load_users_from_yaml", "load_products_from_yaml"]
