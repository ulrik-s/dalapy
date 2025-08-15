import sys
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))
sys.path.append(str(ROOT))

from dalapy import Env, shutdown_env
from example_project import (
    User,
    Product,
    Users,
    Products,
    load_users_from_yaml,
    load_products_from_yaml,
)
from returns.io import IOFailure, IOSuccess


def test_yaml_loading(tmp_path):
    env = Env(db_path=tmp_path / "data.json")
    data_dir = Path(__file__).parent / "data"

    users_yaml = data_dir / "users.yaml"
    products_yaml = data_dir / "products.yaml"

    user_objs = [User(**d) for d in yaml.safe_load(users_yaml.read_text())]
    product_objs = [Product(**d) for d in yaml.safe_load(products_yaml.read_text())]

    res_users = load_users_from_yaml(users_yaml, env)
    res_products = load_products_from_yaml(products_yaml, env)

    assert res_users == [IOSuccess(u) for u in user_objs]
    assert res_products == [IOSuccess(p) for p in product_objs]

    assert Users.list_flow()(env) == IOSuccess(user_objs)
    assert Products.list_flow()(env) == IOSuccess(product_objs)

    res_users_fail = load_users_from_yaml(users_yaml, env)
    res_products_fail = load_products_from_yaml(products_yaml, env)

    assert all(r == IOFailure("id_exists") for r in res_users_fail)
    assert all(r == IOFailure("id_exists") for r in res_products_fail)

    assert isinstance(shutdown_env()(env), IOSuccess)
