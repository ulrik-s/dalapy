import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))
sys.path.append(str(ROOT))

from dalapy import Env, shutdown_env
from example_project import User, Users
from returns.io import IOFailure, IOSuccess


def test_users_repo(tmp_path):
    env = Env(db_path=tmp_path / "data.json")

    user = User(id=1, name="Alice", spend=10.0)
    assert Users.create_flow(user)(env) == IOSuccess(user)

    # Duplicate insert fails
    assert Users.create_flow(user)(env) == IOFailure("id_exists")

    # Unique case-insensitive name
    dup_name = User(id=2, name="ALICE")
    assert Users.create_flow(dup_name)(env) == IOFailure("unique_violation:name")

    updated = User(id=1, name="Alice", spend=20.0)
    assert Users.update_flow(updated)(env) == IOSuccess(updated)

    # Insert second user and test lookups
    bob = User(id=2, name="Bob", spend=5.0)
    assert Users.create_flow(bob)(env) == IOSuccess(bob)
    assert Users.lookup_id_by_flow("name", "bob", nocase=True)(env) == IOSuccess(2)
    assert Users.get_by_unique_flow("name", "Bob")(env) == IOSuccess(bob)

    assert Users.get_flow(1)(env) == IOSuccess(updated)
    assert Users.list_flow()(env) == IOSuccess([updated, bob])

    assert Users.delete_flow(1)(env) == IOSuccess(1)
    assert Users.get_flow(1)(env) == IOFailure("not_found")

    assert isinstance(shutdown_env()(env), IOSuccess)
