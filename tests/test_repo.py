import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))
sys.path.append(str(ROOT))

from dalapy import Env, Repo, collection_for
from example_project import User
from returns.io import IOFailure, IOSuccess


def test_repo_crud(tmp_path):
    env = Env(data_path=tmp_path / "data.json")
    repo = Repo(collection_for(User, "users"))

    user = User(id=1, name="Alice", spend=10.0)
    assert repo.create(env, user) == IOSuccess(user)

    # Duplicate insert fails
    assert repo.create(env, user) == IOFailure("id exists")

    updated = User(id=1, name="Alice", spend=20.0)
    assert repo.update(env, updated) == IOSuccess(updated)

    assert repo.get(env, 1) == IOSuccess(updated)
    assert repo.list(env) == IOSuccess([updated])
    assert repo.delete(env, 1) == IOSuccess(1)
    assert repo.get(env, 1) == IOFailure("not found")
