import sys
from pathlib import Path
from dataclasses import replace

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))
sys.path.append(str(ROOT))

import yaml
from dalapy import Env, shutdown_env
from example_project import (
    Product,
    ProductGroup,
    DataAPI,
    load_products_from_yaml,
    load_config_from_yaml,
)
from returns.io import IOSuccess


def test_config_loader(tmp_path):
    env = Env(db_path=tmp_path / "data.json")
    api = DataAPI(env)
    data_dir = Path(__file__).parent / "data"

    products_yaml = data_dir / "products.yaml"
    config_yaml = data_dir / "config.yml"

    product_objs = [Product(**d) for d in yaml.safe_load(products_yaml.read_text())]
    load_products_from_yaml(products_yaml, api)

    pg1 = ProductGroup(id=1, tag="widget", path="products/widget")
    pg2 = ProductGroup(id=2, tag="gadget", path="products/gadget")
    expected_p1 = replace(product_objs[0], tag="widget")
    expected_p2 = replace(product_objs[1], tag="gadget")

    res = load_config_from_yaml(config_yaml, api)
    assert res["product_groups"] == [IOSuccess(pg1), IOSuccess(pg2)]
    assert res["products"] == [IOSuccess(expected_p1), IOSuccess(expected_p2)]

    assert api.list_product_groups() == IOSuccess([pg1, pg2])
    assert api.list_products() == IOSuccess([expected_p1, expected_p2])

    assert isinstance(shutdown_env()(env), IOSuccess)
