import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))
sys.path.append(str(ROOT))

from dalapy import Env, shutdown_env
from example_project import Product, Products
from returns.io import IOFailure, IOSuccess


def test_products_repo(tmp_path):
    env = Env(db_path=tmp_path / "products.json")

    prod = Product(id=1, sku="ABC", price=100.0)
    assert Products.upsert_flow(prod)(env) == IOSuccess(prod)

    dup_case = Product(id=2, sku="abc")
    assert Products.create_flow(dup_case)(env) == IOSuccess(dup_case)

    dup = Product(id=3, sku="ABC")
    assert Products.create_flow(dup)(env) == IOFailure("unique_violation:sku")

    updated = Product(id=1, sku="ABC", price=150.0)
    assert Products.update_flow(updated)(env) == IOSuccess(updated)

    assert Products.get_by_flow("sku", "abc")(env) == IOSuccess(dup_case)
    assert Products.lookup_id_by_flow("sku", "ABC")(env) == IOSuccess(1)
    assert Products.exists_by_flow("sku", "ABC", nocase=False)(env) == IOSuccess(True)
    assert Products.list_flow()(env) == IOSuccess([updated, dup_case])

    assert Products.delete_flow(1)(env) == IOSuccess(1)
    assert Products.get_flow(1)(env) == IOFailure("not_found")

    assert isinstance(shutdown_env()(env), IOSuccess)
