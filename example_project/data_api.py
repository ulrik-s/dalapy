"""Central data API combining CRUD and complex queries."""

from __future__ import annotations

from typing import List, Set, Tuple

from dataclasses import replace
from returns.io import IOResult, IOSuccess, IOFailure

from dalapy import Env
from .models import User, Product, System, ProductGroup
from .repositories import Users, Products, Systems, ProductGroups


class DataAPI:
    """High level data API ensuring consistent access to the DB."""

    def __init__(self, env: Env):
        self.env = env

    # ---- Users ---------------------------------------------------------
    def create_user(self, user: User) -> IOResult[User, str]:
        return Users.create_flow(user)(self.env)

    def list_users(self) -> IOResult[List[User], str]:
        return Users.list_flow()(self.env)

    def get_user(self, user_id: int) -> IOResult[User, str]:
        return Users.get_flow(user_id)(self.env)

    def get_user_by_name(self, name: str) -> IOResult[User, str]:
        return Users.get_by_unique_flow("name", name)(self.env)

    def update_user(self, user_id: int, **fields) -> IOResult[User, str]:
        def _update(user: User) -> IOResult[User, str]:
            updated = replace(user, **fields)
            return Users.update_flow(updated)(self.env)

        return self.get_user(user_id).bind(_update)

    # ---- Products ------------------------------------------------------
    def create_product(self, product: Product) -> IOResult[Product, str]:
        return Products.create_flow(product)(self.env)

    def list_products(self) -> IOResult[List[Product], str]:
        return Products.list_flow()(self.env)

    def get_product(self, product_id: int) -> IOResult[Product, str]:
        return Products.get_flow(product_id)(self.env)

    def get_product_by_sku(self, sku: str) -> IOResult[Product, str]:
        return Products.get_by_unique_flow("sku", sku)(self.env)

    def list_product_versions(self) -> IOResult[Set[str], str]:
        return Products.list_flow()(self.env).map(
            lambda products: {p.version for p in products if p.version}
        )

    def update_product(self, product_id: int, **fields) -> IOResult[Product, str]:
        def _update(product: Product) -> IOResult[Product, str]:
            updated = replace(product, **fields)
            return Products.update_flow(updated)(self.env)

        return self.get_product(product_id).bind(_update)

    # ---- Product Groups -------------------------------------------------
    def create_product_group(self, group: ProductGroup) -> IOResult[ProductGroup, str]:
        return ProductGroups.create_flow(group)(self.env)

    def list_product_groups(self) -> IOResult[List[ProductGroup], str]:
        return ProductGroups.list_flow()(self.env)

    def get_product_group(self, group_id: int) -> IOResult[ProductGroup, str]:
        return ProductGroups.get_flow(group_id)(self.env)

    def get_product_group_by_tag(self, tag: str) -> IOResult[ProductGroup, str]:
        return ProductGroups.get_by_unique_flow("tag", tag)(self.env)

    def update_product_group(self, group_id: int, **fields) -> IOResult[ProductGroup, str]:
        def _update(group: ProductGroup) -> IOResult[ProductGroup, str]:
            updated = replace(group, **fields)
            return ProductGroups.update_flow(updated)(self.env)

        return self.get_product_group(group_id).bind(_update)

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

    def get_products_for_system(self, system_name: str) -> IOResult[List[Product], str]:
        def _collect(system: System) -> IOResult[List[Product], str]:
            result: IOResult[List[Product], str] = IOSuccess([])
            for pid in system.product_ids:
                result = result.bind(
                    lambda acc, pid=pid: self.get_product(pid).map(lambda p: acc + [p])
                )
            return result

        return self.get_system_by_name(system_name).bind(_collect)

    def list_product_skus_prices_for_system(self, system_name: str) -> IOResult[List[Tuple[str, float]], str]:
        return self.get_products_for_system(system_name).map(
            lambda products: [(p.sku, p.price) for p in products]
        )

    def get_product_in_system_by_version(self, system_name: str, version: str) -> IOResult[Product, str]:
        def _find(products: List[Product]) -> IOResult[Product, str]:
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
