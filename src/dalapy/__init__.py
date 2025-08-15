from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Generic, List, Optional, Protocol, Tuple, TypeVar

from pydantic import ConfigDict, TypeAdapter
from pydantic.dataclasses import dataclass as pyd_dataclass

from tinydb import TinyDB, Query, where
from tinydb.table import Table
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
from filelock import FileLock

from returns.io import IOResult, IOSuccess, IOFailure
from returns.context import RequiresContextIOResult as RCIOResult
from returns.pipeline import flow
from returns.pointfree import bind
from returns.curry import curry


# ===================== Domain =====================

class HasId(Protocol):
    id: int

T = TypeVar('T', bound=HasId)
A = TypeVar('A')


@pyd_dataclass(config=ConfigDict(extra='ignore'))
class User(HasId):
    id: int
    name: str
    spend: float = 0.0
    email: Optional[str] = None


@pyd_dataclass(config=ConfigDict(extra='ignore'))
class Product(HasId):
    id: int
    sku: str
    price: float = 0.0
    currency: str = "SEK"


# ===================== Env =====================

@pyd_dataclass(config=ConfigDict(extra='ignore', arbitrary_types_allowed=True))
class Env:
    db_path: Path
    lock_path: Optional[Path] = None
    db: Optional[TinyDB] = None


def _open_db(env: Env) -> IOResult[Env, str]:
    try:
        if env.db is None:
            env.db = TinyDB(
                env.db_path,
                storage=CachingMiddleware(JSONStorage),
                ensure_ascii=False,
                indent=2,
            )
        return IOSuccess(env)
    except Exception as exc:
        return IOFailure(f'db_open_error: {exc}')


def _close_db(env: Env) -> IOResult[Env, str]:
    try:
        if env.db is not None:
            env.db.close()  # flush + close
            env.db = None
        return IOSuccess(env)
    except Exception as exc:
        return IOFailure(f'db_close_error: {exc}')


def ensure_env() -> RCIOResult[Env, str, Env]:
    return RCIOResult(_open_db)


def shutdown_env() -> RCIOResult[Env, str, Env]:
    return RCIOResult(_close_db)


# ===================== Spec & validation =====================

@pyd_dataclass(config=ConfigDict(extra='ignore'))
class UniqueRule:
    field: str
    nocase: bool = False
    allow_none: bool = True  # if False, None is treated as a real value (must be unique or blocked)


# Return an error string to block the write, or None if ok.
CustomValidator = Callable[[Env, Table, T, Optional[int]], Optional[str]]


@pyd_dataclass(config=ConfigDict(extra='ignore', arbitrary_types_allowed=True))
class RepoSpec(Generic[T]):
    table_name: str
    adapter: TypeAdapter            # TypeAdapter[T]
    unique: Tuple[UniqueRule, ...] = ()
    validator: Optional[CustomValidator] = None  # extra per-collection rules


# ===================== DRY helpers =====================

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
    """
    Centralized I/O: table lookup, optional file lock, exec, flush, and error mapping.
    """
    def _io(env: Env) -> IOResult[A, str]:
        try:
            tbl = _table(env, spec)
            if write:
                with _with_lock(env):
                    out = op(env, tbl)
                    # ensure write-through (CachingMiddleware flush)
                    env.db.storage.flush()  # type: ignore[attr-defined]
                    return IOSuccess(out)
            else:
                return IOSuccess(op(env, tbl))
        except RuntimeError as e:
            # domain-ish errors we raised intentionally
            return IOFailure(str(e))
        except Exception as exc:
            return IOFailure(f'db_error: {exc}')
    return RCIOResult(_io)


def _norm(rule: UniqueRule, value: Any):
    if value is None:
        return None
    return str(value).lower() if rule.nocase else value


def _validate(env: Env, tbl: Table, spec: RepoSpec[T], obj: T, *, exclude_id: Optional[int]) -> Optional[str]:
    # Unique rules
    for rule in spec.unique:
        val = _norm(rule, getattr(obj, rule.field, None))
        if val is None and rule.allow_none:
            continue
        for row in tbl:
            if exclude_id is not None and row.get('id') == exclude_id:
                continue
            if _norm(rule, row.get(rule.field)) == val:
                return f'unique_violation:{rule.field}'
    # Extra per-collection rules
    if spec.validator:
        err = spec.validator(env, tbl, obj, exclude_id)
        if err:
            return err
    return None


# ===================== Generic Repo =====================

class Repo(Generic[T]):
    def __init__(self, spec: RepoSpec[T]):
        self.spec = spec

    # ---- write ops ----
    @curry
    def create(self, obj: T, _env: Env) -> RCIOResult[Env, str, T]:
        def _op(env: Env, tbl: Table) -> T:
            if any(d.get('id') == obj.id for d in tbl):
                raise RuntimeError('id_exists')
            err = _validate(env, tbl, self.spec, obj, exclude_id=None)
            if err:
                raise RuntimeError(err)
            tbl.insert(self.spec.adapter.dump_python(obj))
            return obj
        return _run(self.spec, True, _op)

    @curry
    def upsert(self, obj: T, _env: Env) -> RCIOResult[Env, str, T]:
        def _op(env: Env, tbl: Table) -> T:
            err = _validate(env, tbl, self.spec, obj, exclude_id=obj.id)
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
            err = _validate(env, tbl, self.spec, obj, exclude_id=obj.id)
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

    # ---- read ops ----
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

    # ---- field-based lookups (to find the id or entity by other fields) ----
    @curry
    def get_by(self, field: str, value: Any, nocase: bool, _env: Env
               ) -> RCIOResult[Env, str, T]:
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
    def lookup_id_by(self, field: str, value: Any, nocase: bool, _env: Env
                     ) -> RCIOResult[Env, str, int]:
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

    def exists_by(self, field: str, value: Any, nocase: bool, _env: Env
                  ) -> RCIOResult[Env, str, bool]:
        def _op(env: Env, tbl: Table) -> bool:
            if nocase and isinstance(value, str):
                return tbl.contains(where(field).test(lambda v: isinstance(v, str) and v.lower() == value.lower()))
            return tbl.contains(where(field) == value)
        return _run(self.spec, False, _op)

    @curry
    def get_by_unique(self, field: str, value: Any, _env: Env) -> RCIOResult[Env, str, T]:
        nocase = any(r.field == field and r.nocase for r in self.spec.unique)
        return self.get_by(field, value, nocase, _env)


# ===================== RepoFactory =====================

@pyd_dataclass(config=ConfigDict(extra='ignore', arbitrary_types_allowed=True))
class RepoAPI(Generic[T]):
    spec: RepoSpec[T]
    repo: Repo[T]

    # lambda-free flows:
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

    def get_by_flow(self, field: str, value: Any, nocase: bool = False
                    ) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.get_by(field, value, nocase))

    def lookup_id_by_flow(self, field: str, value: Any, nocase: bool = False
                          ) -> RCIOResult[Env, str, int]:
        return ensure_env().bind(self.repo.lookup_id_by(field, value, nocase))

    def get_by_unique_flow(self, field: str, value: Any
                           ) -> RCIOResult[Env, str, T]:
        return ensure_env().bind(self.repo.get_by_unique(field, value))


def repo_factory(spec: RepoSpec[T]) -> RepoAPI[T]:
    return RepoAPI(spec=spec, repo=Repo(spec))


# ===================== Specs & factory instances =====================

USER_SPEC = RepoSpec[User](
    table_name='users',
    adapter=TypeAdapter(User),
    unique=(UniqueRule('name', nocase=True),),  # case-insensitive unique name
)

PRODUCT_SPEC = RepoSpec[Product](
    table_name='products',
    adapter=TypeAdapter(Product),
    unique=(UniqueRule('sku', nocase=False),),  # SKU unique (case-sensitive)
)

Users = repo_factory(USER_SPEC)
Products = repo_factory(PRODUCT_SPEC)


# ===================== Demo =====================

if __name__ == '__main__':
    env = Env(db_path=Path('./var/tiny.json'))
    u = User(id=1, name='Ulrik', spend=280.0)

    print(Users.create_flow(u)(env))
    print(Users.list_flow()(env))
    print(Users.get_by_unique_flow('name', 'ulrik')(env))  # nocase unique lookup
    rid = Users.lookup_id_by_flow('name', 'Ulrik', nocase=True)(env)
    print(rid)
    if rid._inner_value is not None:
        print(Users.get_flow(rid._inner_value)(env))  # id-based discipline

    # Products
    p = Product(id=10, sku='ABC-123', price=199.0)
    print(Products.upsert_flow(p)(env))
    print(Products.get_by_flow('sku', 'ABC-123')(env))
    print(Products.list_flow()(env))

    print(shutdown_env()(env))
