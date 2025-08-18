"""Central data API combining CRUD and complex queries."""

from __future__ import annotations

from typing import List, Set, Tuple

from dataclasses import replace
from returns.io import IOResult, IOSuccess, IOFailure

from dalapy import Env
from .models import CSModule, System, CSModuleGroup
from .repositories import Products, Systems, ProductGroups


class DataAPI:
    """High level data API ensuring consistent access to the DB."""

    def __init__(self, env: Env):
        self.env = env

    # ---- Products ------------------------------------------------------
    def create_cs_module(self, product: CSModule) -> IOResult[CSModule, str]:
        return Products.create_flow(product)(self.env)

    def list_cs_modules(self) -> IOResult[List[CSModule], str]:
        return Products.list_flow()(self.env)

    def get_cs_module(self, product_id: int) -> IOResult[CSModule, str]:
        return Products.get_flow(product_id)(self.env)

    def get_cs_module_by_sku(self, sku: str) -> IOResult[CSModule, str]:
        return Products.get_by_unique_flow("sku", sku)(self.env)

    def list_cs_module_versions(self) -> IOResult[Set[str], str]:
        return Products.list_flow()(self.env).map(
            lambda products: {p.version for p in products if p.version}
        )

    def update_cs_module(self, product_id: int, **fields) -> IOResult[CSModule, str]:
        def _update(product: CSModule) -> IOResult[CSModule, str]:
            updated = replace(product, **fields)
            return Products.update_flow(updated)(self.env)

        return self.get_cs_module(product_id).bind(_update)

    # ---- Product Groups -------------------------------------------------
    def create_cs_module_group(self, group: CSModuleGroup) -> IOResult[CSModuleGroup, str]:
        return ProductGroups.create_flow(group)(self.env)

    def list_cs_module_groups(self) -> IOResult[List[CSModuleGroup], str]:
        return ProductGroups.list_flow()(self.env)

    def get_cs_module_group(self, group_id: int) -> IOResult[CSModuleGroup, str]:
        return ProductGroups.get_flow(group_id)(self.env)

    def get_cs_module_group_by_tag(self, tag: str) -> IOResult[CSModuleGroup, str]:
        return ProductGroups.get_by_unique_flow("tag", tag)(self.env)

    def update_cs_module_group(self, group_id: int, **fields) -> IOResult[CSModuleGroup, str]:
        def _update(group: CSModuleGroup) -> IOResult[CSModuleGroup, str]:
            updated = replace(group, **fields)
            return ProductGroups.update_flow(updated)(self.env)

        return self.get_cs_module_group(group_id).bind(_update)

    # ---- Systems -------------------------------------------------------
    def create_system(self, system: System) -> IOResult[System, str]:
        for pid in system.product_ids:
            exists_res = Products.exists_by_flow("id", pid)(self.env).bind(
                lambda exists: IOSuccess(True) if exists else IOFailure("missing_product")
            )
            if isinstance(exists_res, IOFailure):
                return exists_res
        return Systems.create_flow(system)(self.env)

    def list_systems(self) -> IOResult[List[System], str]:
        return Systems.list_flow()(self.env)

    def get_system(self, system_id: int) -> IOResult[System, str]:
        return Systems.get_flow(system_id)(self.env)

    def get_system_by_name(self, name: str) -> IOResult[System, str]:
        return Systems.get_by_flow("name", name, nocase=True)(self.env)

    def list_system_names(self) -> IOResult[List[str], str]:
        return self.list_systems().map(lambda systems: [s.name for s in systems])

    def get_products_for_system(self, system_name: str) -> IOResult[List[CSModule], str]:
        def _collect(system: System) -> IOResult[List[CSModule], str]:
            result: IOResult[List[CSModule], str] = IOSuccess([])
            for pid in system.product_ids:
                result = result.bind(
                    lambda acc, pid=pid: self.get_cs_module(pid).map(lambda p: acc + [p])
                )
            return result

        return self.get_system_by_name(system_name).bind(_collect)

    def list_product_skus_prices_for_system(self, system_name: str) -> IOResult[List[Tuple[str, float]], str]:
        return self.get_products_for_system(system_name).map(
            lambda products: [(p.sku, p.price) for p in products]
        )

    def get_product_in_system_by_version(self, system_name: str, version: str) -> IOResult[CSModule, str]:
        def _find(products: List[CSModule]) -> IOResult[CSModule, str]:
            for p in products:
                if p.version == version:
                    return IOSuccess(p)
            return IOFailure("not_found")

        return self.get_products_for_system(system_name).bind(_find)

    def update_system(self, system_id: int, **fields) -> IOResult[System, str]:
        def _update(system: System) -> IOResult[System, str]:
            updated = replace(system, **fields)
            for pid in updated.product_ids:
                exists_res = Products.exists_by_flow("id", pid)(self.env).bind(
                    lambda exists: IOSuccess(True) if exists else IOFailure("missing_product")
                )
                if isinstance(exists_res, IOFailure):
                    return exists_res
            return Systems.update_flow(updated)(self.env)

        return self.get_system(system_id).bind(_update)


__all__ = ["DataAPI"]
