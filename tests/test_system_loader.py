import sys
from pathlib import Path
import tarfile
import io
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))
sys.path.append(str(ROOT))

from dalapy import Env, shutdown_env
from example_project import (
    Product,
    System,
    DataAPI,
    load_system_from_yaml,
)
from returns.io import IOFailure, IOSuccess


def _make_bundle(path: Path, data: dict) -> None:
    """Create a tar.gz containing a single product.yml with given data."""
    content = yaml.safe_dump(data).encode("utf-8")
    info = tarfile.TarInfo("product.yml")
    info.size = len(content)
    with tarfile.open(path, "w:gz") as tar:
        tar.addfile(info, io.BytesIO(content))


def test_system_loading(tmp_path):
    env = Env(db_path=tmp_path / "data.json")
    api = DataAPI(env)

    # prepare product bundles and system.yml in a temp directory
    data_dir = Path(__file__).parent / "data"
    system_yaml = tmp_path / "system.yml"
    system_yaml.write_text((data_dir / "system.yml").read_text())

    _make_bundle(
        tmp_path / "widget-1.0.tar.gz",
        {"id": 1, "sku": "WID", "price": 50.0, "currency": "SEK"},
    )
    _make_bundle(
        tmp_path / "gadget-2.0.tar.gz",
        {"id": 2, "sku": "GAD", "price": 75.0, "currency": "SEK"},
    )

    product1 = Product(id=1, sku="WID", price=50.0, currency="SEK", version="1.0")
    product2 = Product(id=2, sku="GAD", price=75.0, currency="SEK", version="2.0")
    system_obj = System(id=1, name="DemoSystem", product_ids=[1, 2])

    prod_res, sys_res = load_system_from_yaml(system_yaml, api)
    assert prod_res == [IOSuccess(product1), IOSuccess(product2)]
    assert sys_res == IOSuccess(system_obj)

    assert api.list_products() == IOSuccess([product1, product2])
    assert api.list_systems() == IOSuccess([system_obj])

    # query which systems are present
    assert api.list_system_names() == IOSuccess(["DemoSystem"])

    # query product versions stored
    assert api.list_product_versions() == IOSuccess({"1.0", "2.0"})

    # query products belonging to a system and list their sku and price
    skus_prices = api.list_product_skus_prices_for_system("DemoSystem")
    assert skus_prices.map(set) == IOSuccess({("WID", 50.0), ("GAD", 75.0)})

    assert api.get_product_in_system_by_version("DemoSystem", "1.0") == IOSuccess(product1)
    assert api.get_product_in_system_by_version("DemoSystem", "2.0") == IOSuccess(product2)

    prod_res_fail, sys_res_fail = load_system_from_yaml(system_yaml, api)
    assert all(r == IOFailure("id_exists") for r in prod_res_fail)
    assert sys_res_fail == IOFailure("id_exists")

    assert isinstance(shutdown_env()(env), IOSuccess)
