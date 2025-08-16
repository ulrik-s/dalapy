"""Central data API combining CRUD and complex queries."""

from __future__ import annotations

from typing import List, Set, Tuple

from returns.io import IOResult, IOSuccess, IOFailure

from dalapy import Env
from .models import User, Product, System
from .repositories import Users, Products, Systems


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
        res = Products.list_flow()(self.env)
        if isinstance(res, IOFailure):
            return res
        products = res.unwrap()._inner_value
        return IOSuccess({p.version for p in products if p.version})

    # ---- Systems -------------------------------------------------------
    def create_system(self, system: System) -> IOResult[System, str]:
        for pid in system.product_ids:
            exists_res = Products.exists_by_flow("id", pid)(self.env)
            if isinstance(exists_res, IOFailure) or not exists_res.unwrap()._inner_value:
                return IOFailure("missing_product")
        return Systems.create_flow(system)(self.env)

    def list_systems(self) -> IOResult[List[System], str]:
        return Systems.list_flow()(self.env)

    def get_system(self, system_id: int) -> IOResult[System, str]:
        return Systems.get_flow(system_id)(self.env)

    def get_system_by_name(self, name: str) -> IOResult[System, str]:
        return Systems.get_by_flow("name", name, nocase=True)(self.env)

    def list_system_names(self) -> IOResult[List[str], str]:
        res = self.list_systems()
        if isinstance(res, IOFailure):
            return res
        systems = res.unwrap()._inner_value
        return IOSuccess([s.name for s in systems])

    def get_products_for_system(self, system_name: str) -> IOResult[List[Product], str]:
        sys_res = self.get_system_by_name(system_name)
        if isinstance(sys_res, IOFailure):
            return sys_res
        system = sys_res.unwrap()._inner_value
        products: List[Product] = []
        for pid in system.product_ids:
            p_res = self.get_product(pid)
            if isinstance(p_res, IOFailure):
                return p_res
            products.append(p_res.unwrap()._inner_value)
        return IOSuccess(products)

    def list_product_skus_prices_for_system(self, system_name: str) -> IOResult[List[Tuple[str, float]], str]:
        prods_res = self.get_products_for_system(system_name)
        if isinstance(prods_res, IOFailure):
            return prods_res
        products = prods_res.unwrap()._inner_value
        return IOSuccess([(p.sku, p.price) for p in products])

    def get_product_in_system_by_version(self, system_name: str, version: str) -> IOResult[Product, str]:
        prods_res = self.get_products_for_system(system_name)
        if isinstance(prods_res, IOFailure):
            return prods_res
        for p in prods_res.unwrap()._inner_value:
            if p.version == version:
                return IOSuccess(p)
        return IOFailure("not_found")


__all__ = ["DataAPI"]
