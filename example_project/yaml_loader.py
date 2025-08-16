"""Utility functions to load YAML data into the example project repos."""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import yaml
import tarfile
import re
from returns.io import IOResult

from .data_api import DataAPI
from .models import User, Product, System


def load_users_from_yaml(path: Path, api: DataAPI) -> List[IOResult[User, str]]:
    """Read a YAML file of users and insert them into the database."""
    data = yaml.safe_load(path.read_text()) or []
    results: List[IOResult[User, str]] = []
    for item in data:
        user = User(**item)
        results.append(api.create_user(user))
    return results


def load_products_from_yaml(path: Path, api: DataAPI) -> List[IOResult[Product, str]]:
    """Read a YAML file of products and insert them into the database."""
    data = yaml.safe_load(path.read_text()) or []
    results: List[IOResult[Product, str]] = []
    for item in data:
        product = Product(**item)
        results.append(api.create_product(product))
    return results


def load_system_from_yaml(
    path: Path, api: DataAPI
) -> Tuple[List[IOResult[Product, str]], IOResult[System, str]]:
    """Read a system YAML referencing product tarballs and insert them."""
    data = yaml.safe_load(path.read_text()) or {}
    product_results: List[IOResult[Product, str]] = []
    product_ids: List[int] = []
    for ref in data.get("products", []):
        p_path = path.parent / ref
        m = re.search(r"-(?P<version>[\d\.]+)\.tar\.gz$", p_path.name)
        version = m.group("version") if m else ""
        with tarfile.open(p_path, "r:gz") as tar:
            member = tar.getmember("product.yml")
            file = tar.extractfile(member)
            assert file is not None
            pdata = yaml.safe_load(file.read().decode()) or {}
        pdata["version"] = version
        product = Product(**pdata)
        product_results.append(api.create_product(product))
        product_ids.append(product.id)
    system = System(id=data["id"], name=data["name"], product_ids=product_ids)
    system_result = api.create_system(system)
    return product_results, system_result


__all__ = ["load_users_from_yaml", "load_products_from_yaml", "load_system_from_yaml"]
