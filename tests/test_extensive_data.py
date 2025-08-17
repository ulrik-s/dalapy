import sys
from pathlib import Path
import tarfile
import io
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))
sys.path.append(str(ROOT))

from dalapy import Env, shutdown_env
from example_project import DataAPI, load_system_from_yaml, load_config_from_yaml
from returns.io import IOResult


def _make_bundle(path: Path, data: dict) -> None:
    """Create a tar.gz containing a single product.yml with given data."""
    content = yaml.safe_dump(data).encode("utf-8")
    info = tarfile.TarInfo("product.yml")
    info.size = len(content)
    with tarfile.open(path, "w:gz") as tar:
        tar.addfile(info, io.BytesIO(content))


def _success(res: IOResult) -> bool:
    return res.map(lambda _: True).value_or(False)


def test_extensive_systems_and_config(tmp_path, capsys):
    env = Env(db_path=tmp_path / "data.json")
    api = DataAPI(env)

    products_info = []
    pid = 1
    for g in range(1, 6):
        for p in range(1, 6):
            sku = f"G{g}P{p}"
            price = float(g * 100 + p)
            tag = f"group{g}"
            version = f"{p}.0"
            tar_name = f"{sku}-{version}.tar.gz"
            _make_bundle(
                tmp_path / tar_name,
                {
                    "id": pid,
                    "sku": sku,
                    "price": price,
                    "currency": "SEK",
                    "tag": tag,
                },
            )
            products_info.append({"id": pid, "tar": tar_name})
            pid += 1

    systems_data = [
        {"id": 1, "name": "SystemA", "products": [p["tar"] for p in products_info[0:10]]},
        {"id": 2, "name": "SystemB", "products": [p["tar"] for p in products_info[10:20]]},
        {"id": 3, "name": "SystemC", "products": [p["tar"] for p in products_info[20:25]]},
    ]
    for sd in systems_data:
        sys_yaml = tmp_path / f"system{sd['id']}.yml"
        sys_yaml.write_text(yaml.safe_dump(sd))
        prod_res, sys_res = load_system_from_yaml(sys_yaml, api)
        assert all(_success(r) for r in prod_res)
        assert _success(sys_res)

    config_data = {
        "product_groups": [
            {"id": 1, "tag": "group1", "path": "products/group1"},
            {"id": 2, "tag": "group2", "path": "products/group2"},
            {"id": 3, "tag": "group3", "path": "products/group3"},
            {"id": 4, "tag": "group4", "path": "products/group4"},
            {"id": 5, "tag": "group5", "path": "products/group5"},
        ],
        "products": [{"id": i, "tag": "group2"} for i in range(1, 6)]
        + [{"id": i, "tag": "group1"} for i in range(6, 11)],
    }
    config_yaml = tmp_path / "config.yml"
    config_yaml.write_text(yaml.safe_dump(config_data))
    config_res = load_config_from_yaml(config_yaml, api)
    assert all(_success(r) for r in config_res["product_groups"])
    assert all(_success(r) for r in config_res["products"])

    products_res = api.list_products()
    groups_res = api.list_product_groups()
    systems_res = api.list_systems()
    assert _success(products_res)
    assert _success(groups_res)
    assert _success(systems_res)
    products = products_res.unwrap()._inner_value
    groups = groups_res.unwrap()._inner_value
    systems = systems_res.unwrap()._inner_value

    tag_map = {}
    for prod in products:
        tag_map.setdefault(prod.tag, []).append(prod)
    for pg in groups:
        assert len(tag_map.get(pg.tag, [])) >= 5

    print("Systems:")
    for s in systems:
        print(f"{s.name}: {s.product_ids}")
    print("Product Groups:")
    for pg in groups:
        print(f"{pg.path}/")
        for prod in sorted(tag_map.get(pg.tag, []), key=lambda x: x.id):
            print(f"  {prod.sku}")
    captured = capsys.readouterr()
    assert "Systems:" in captured.out
    assert "Product Groups:" in captured.out

    # Ensure the tree structure is visible in test logs when running pytest -q
    with capsys.disabled():
        print(captured.out)

    assert _success(shutdown_env()(env))
