"""Utility functions to load YAML data into the example project repos."""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import yaml
import tarfile
import re
from returns.io import IOResult

from .data_api import DataAPI
from .models import CSModule, System


def load_cs_modules_from_yaml(path: Path, api: DataAPI) -> List[IOResult[Product, str]]:
    """Read a YAML file of products and insert them into the database."""
    data = yaml.safe_load(path.read_text()) or []
    results: List[IOResult[CSModule, str]] = []
    for item in data:
        product = CSModule(**item)
        results.append(api.create_product(product))
    return results


def load_system_from_yaml(
    path: Path, api: DataAPI
) -> Tuple[List[IOResult[CSModule, str]], IOResult[System, str]]:
    """Read a system YAML referencing product tarballs and insert them."""
    data = yaml.safe_load(path.read_text()) or {}
    product_results: List[IOResult[CSModule, str]] = []
    product_ids: List[int] = []
    for ref in data.get("cs_modules", []):
        p_path = path.parent / ref
        m = re.search(r"-(?P<version>[\d\.]+)\.tar\.gz$", p_path.name)
        version = m.group("version") if m else ""
        with tarfile.open(p_path, "r:gz") as tar:
            member = tar.getmember("product.yml")
            file = tar.extractfile(member)
            assert file is not None
            pdata = yaml.safe_load(file.read().decode()) or {}
        pdata["version"] = version
        product = CSModule(**pdata)
        product_results.append(api.create_cs_module(product))
        product_ids.append(product.id)
    system = System(id=data["id"], name=data["name"], csmodule_ids=product_ids)
    system_result = api.create_system(system)
    return product_results, system_result


__all__ = [
    "load_cs_modules_from_yaml",
    "load_system_from_yaml"
]