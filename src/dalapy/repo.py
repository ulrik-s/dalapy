from __future__ import annotations

from typing import Any, Callable, Generic, List, TypeVar

from tinydb import Query, where
from tinydb.table import Table
from filelock import FileLock

from returns.io import IOResult, IOSuccess, IOFailure
from returns.context import RequiresContextIOResult as RCIOResult
from returns.curry import curry

from .env import Env, ensure_env
from .spec import RepoSpec, validate

T = TypeVar('T')
A = TypeVar('A')


def _table(env: Env, spec: RepoSpec[T]) -> Table:
    assert env.db is not None, "DB not opened"
    return env.db.table(spec.table_name)


def _with_lock(env: Env):
    if env.lock_path:
        return FileLock(str(env.lock_path))
    class _Noop:
        def __enter__(self): return None
        def __exit__(self, *a): return False
    return _Noop()


def _run(spec: RepoSpec[T], write: bool, op: Callable[[Env, Table], A]) -> RCIOResult[Env, str, A]:
    def _io(env: Env) -> IOResult[A, str]:
        try:
            tbl = _table(env, spec)
            if write:
                with _with_lock(env):
                    out = op(env, tbl)
                    env.db.storage.flush()  # type: ignore[attr-defined]
                    return IOSuccess(out)
            else:
                return IOSuccess(op(env, tbl))
        except RuntimeError as e:
            return IOFailure(str(e))
        except Exception as exc:
            return IOFailure(f'db_error: {exc}')
    return RCIOResult(_io)


class Repo(Generic[T]):
    def __init__(self, spec: RepoSpec[T]):
        self.spec = spec

    @curry
    def create(self, obj: T, _env: Env) -> RCIOResult[Env, str, T]:
        def _op(env: Env, tbl: Table) -> T:
            if any(d.get('id') == obj.id for d in tbl):
                raise RuntimeError('id_exists')
            err = validate(env, tbl, self.spec, obj, exclude_id=None)
            if err:
                raise RuntimeError(err)
            tbl.insert(self.spec.adapter.dump_python(obj))
            return obj
        return _run(self.spec, True, _op)

    @curry
    def upsert(self, obj: T, _env: Env) -> RCIOResult[Env, str, T]:
        def _op(env: Env, tbl: Table) -> T:
            err = validate(env, tbl, self.spec, obj, exclude_id=obj.id)
            if err:
                raise RuntimeError(err)
            q = Query()
            tbl.upsert(self.spec.adapter.dump_python(obj), q.id == obj.id)
            return obj
        return _run(self.spec, True, _op)

    @curry
    def update(self, obj: T, _env: Env) -> RCIOResult[Env, str, T]:
        def _op(env: Env, tbl: Table) -> T:
            q = Query()
            if not tbl.contains(q.id == obj.id):
                raise RuntimeError('not_found')
            err = validate(env, tbl, self.spec, obj, exclude_id=obj.id)
            if err:
                raise RuntimeError(err)
            tbl.update(self.spec.adapter.dump_python(obj), q.id == obj.id)
            return obj
        return _run(self.spec, True, _op)

    @curry
    def delete(self, entity_id: int, _env: Env) -> RCIOResult[Env, str, int]:
        def _op(env: Env, tbl: Table) -> int:
            q = Query()
            removed = tbl.remove(q.id == entity_id)
            if removed == []:
                raise RuntimeError('not_found')
            return entity_id
        return _run(self.spec, True, _op)

    @curry
    def get(self, entity_id: int, _env: Env) -> RCIOResult[Env, str, T]:
        def _op(env: Env, tbl: Table) -> T:
            q = Query()
            doc = tbl.get(q.id == entity_id)
            if doc is None:
                raise RuntimeError('not_found')
            return self.spec.adapter.validate_python(doc)
        return _run(self.spec, False, _op)

    def list(self, _env: Env) -> RCIOResult[Env, str, List[T]]:
        def _op(env: Env, tbl: Table) -> List[T]:
            return [self.spec.adapter.validate_python(d) for d in tbl.all()]
        return _run(self.spec, False, _op)

    @curry
    def get_by(self, field: str, value: Any, nocase: bool, _env: Env) -> RCIOResult[Env, str, T]:
        def _op(env: Env, tbl: Table) -> T:
            if nocase and isinstance(value, str):
                doc = tbl.get(where(field).test(lambda v: isinstance(v, str) and v.lower() == value.lower()))
            else:
                doc = tbl.get(where(field) == value)
            if doc is None:
                raise RuntimeError('not_found')
            return self.spec.adapter.validate_python(doc)
        return _run(self.spec, False, _op)

    @curry
    def lookup_id_by(self, field: str, value: Any, nocase: bool, _env: Env) -> RCIOResult[Env, str, int]:
        def _op(env: Env, tbl: Table) -> int:
            if nocase and isinstance(value, str):
                doc = tbl.get(where(field).test(lambda v: isinstance(v, str) and v.lower() == value.lower()))
            else:
                doc = tbl.get(where(field) == value)
            if doc is None:
                raise RuntimeError('not_found')
            if 'id' not in doc:
                raise RuntimeError('corrupt_row')
            return int(doc['id'])
        return _run(self.spec, False, _op)

    @curry
    def exists_by(self, field: str, value: Any, nocase: bool, _env: Env) -> RCIOResult[Env, str, bool]:
        def _op(env: Env, tbl: Table) -> bool:
            if nocase and isinstance(value, str):
                return tbl.contains(where(field).test(lambda v: isinstance(v, str) and v.lower() == value.lower()))
            return tbl.contains(where(field) == value)
        return _run(self.spec, False, _op)

    @curry
    def get_by_unique(self, field: str, value: Any, _env: Env) -> RCIOResult[Env, str, T]:
        nocase = any(r.field == field and r.nocase for r in self.spec.unique)
        return self.get_by(field, value, nocase, _env)


class RepoAPI(Generic[T]):
    def __init__(self, spec: RepoSpec[T], repo: Repo[T]):
        self.spec = spec
        self.repo = repo

    def create_flow(self, obj: T) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.create(obj))

    def upsert_flow(self, obj: T) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.upsert(obj))

    def update_flow(self, obj: T) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.update(obj))

    def delete_flow(self, entity_id: int) -> RCIOResult[Env, str, int]:
        return ensure_env().bind(self.repo.delete(entity_id))

    def get_flow(self, entity_id: int) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.get(entity_id))

    def list_flow(self) -> RCIOResult[Env, str, List[T]]:
        return ensure_env().bind(self.repo.list)

    def get_by_flow(self, field: str, value: Any, nocase: bool = False) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.get_by(field, value, nocase))

    def lookup_id_by_flow(self, field: str, value: Any, nocase: bool = False) -> RCIOResult[Env, str, int]:
        return ensure_env().bind(self.repo.lookup_id_by(field, value, nocase))

    def get_by_unique_flow(self, field: str, value: Any) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.get_by_unique(field, value))

    def exists_by_flow(self, field: str, value: Any, nocase: bool = False) -> RCIOResult[Env, str, bool]:
        return ensure_env().bind(self.repo.exists_by(field, value, nocase))


def repo_factory(spec: RepoSpec[T]) -> RepoAPI[T]:
    return RepoAPI(spec, Repo(spec))
