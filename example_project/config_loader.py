"""Loader for mutable configuration stored in YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml
from returns.io import IOResult

from .data_api import DataAPI
from .models import ProductGroup


def load_config_from_yaml(path: Path, api: DataAPI) -> Dict[str, List[IOResult[Any, str]]]:
    """Load configuration data and apply it to the repositories.

    Returns a mapping of section name to list of IOResult objects produced
    when applying the configuration.
    """
    data = yaml.safe_load(path.read_text()) or {}
    results: Dict[str, List[IOResult[Any, str]]] = {}

    # Product groups are standalone entries
    pg_results: List[IOResult[ProductGroup, str]] = []
    for item in data.get("product_groups", []):
        group = ProductGroup(**item)
        pg_results.append(api.create_product_group(group))
    if pg_results:
        results["product_groups"] = pg_results

    # Augment existing objects with extra fields
    mapping = {
        "products": api.update_product,
        "users": api.update_user,
        "systems": api.update_system,
    }
    for key, updater in mapping.items():
        res_list: List[IOResult[Any, str]] = []
        for item in data.get(key, []):
            obj_id = item.get("id")
            extra = {k: v for k, v in item.items() if k != "id"}
            res_list.append(updater(obj_id, **extra))
        if res_list:
            results[key] = res_list

    return results


__all__ = ["load_config_from_yaml"]
