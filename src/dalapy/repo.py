"""Generic repository implementing CRUD operations using ``returns``."""

from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
from typing import Dict, Generic, TypeVar

from filelock import FileLock
from returns.curry import curry
from returns.io import IOResult, IOFailure, IOSuccess
from returns.result import Result, Failure, Success

from .collection import Collection
from .env import Env
from .has_id import HasId
from .store import GenericStore, STORE_ADAPTER

T = TypeVar("T", bound=HasId)
V = TypeVar("V")

# ========= Low-level I/O (atomic, optional lock) =========

def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_store(env: Env) -> IOResult[GenericStore, str]:
    try:
        if not env.data_path.exists():
            return IOSuccess(GenericStore())
        raw = env.data_path.read_bytes()
        return IOSuccess(STORE_ADAPTER.validate_json(raw))
    except Exception as exc:  # pragma: no cover - IO error
        return IOFailure(f"read error: {exc}")


def write_store(env: Env, store: GenericStore) -> IOResult[GenericStore, str]:
    try:
        env.data_path.parent.mkdir(parents=True, exist_ok=True)
        payload = STORE_ADAPTER.dump_json(store, by_alias=False)
        if env.lock_path:
            with FileLock(str(env.lock_path)):
                _atomic_write_bytes(env.data_path, payload)
        else:
            _atomic_write_bytes(env.data_path, payload)
        return IOSuccess(store)
    except Exception as exc:  # pragma: no cover - IO error
        return IOFailure(f"write error: {exc}")

# ========= Generic store transforms (pure, curried) =========

def _coll_view(store: GenericStore, c: Collection[T]) -> Dict[int, dict]:
    return store.data.get(c.name, {})


def _with_coll(store: GenericStore, c: Collection[T], newmap: Dict[int, dict]) -> GenericStore:
    new_data = dict(store.data)
    new_data[c.name] = newmap
    return replace(store, data=new_data)


@curry
def insert(c: Collection[T], obj: T, store: GenericStore) -> Result[GenericStore, str]:
    m = _coll_view(store, c)
    if obj.id in m:
        return Failure("id exists")
    nm = dict(m)
    nm[obj.id] = c.adapter.dump_python(obj)
    return Success(_with_coll(store, c, nm))


@curry
def upsert(c: Collection[T], obj: T, store: GenericStore) -> Result[GenericStore, str]:
    m = _coll_view(store, c)
    nm = dict(m)
    nm[obj.id] = c.adapter.dump_python(obj)
    return Success(_with_coll(store, c, nm))


@curry
def update(c: Collection[T], obj: T, store: GenericStore) -> Result[GenericStore, str]:
    m = _coll_view(store, c)
    if obj.id not in m:
        return Failure("not found")
    nm = dict(m)
    nm[obj.id] = c.adapter.dump_python(obj)
    return Success(_with_coll(store, c, nm))


@curry
def delete(c: Collection[T], entity_id: int, store: GenericStore) -> Result[GenericStore, str]:
    m = _coll_view(store, c)
    if entity_id not in m:
        return Failure("not found")
    nm = dict(m)
    nm.pop(entity_id)
    return Success(_with_coll(store, c, nm))


@curry
def get(c: Collection[T], entity_id: int, store: GenericStore) -> Result[T, str]:
    m = _coll_view(store, c)
    raw = m.get(entity_id)
    if raw is None:
        return Failure("not found")
    return Success(c.adapter.validate_python(raw))


def list_all(c: Collection[T], store: GenericStore) -> Result[list[T], str]:
    m = _coll_view(store, c)
    return Success([c.adapter.validate_python(v) for v in m.values()])

# ========= Repo that “just works” for any entity =========

class Repo(Generic[T]):
    """Repository exposing common operations for a collection."""

    def __init__(self, coll: Collection[T]):
        self.c = coll

    def create(self, env: Env, obj: T) -> IOResult[T, str]:
        return read_store(env).bind_result(insert(self.c, obj)).bind(
            lambda store: write_store(env, store).map(lambda _: obj)
        )

    def upsert(self, env: Env, obj: T) -> IOResult[T, str]:
        return read_store(env).bind_result(upsert(self.c, obj)).bind(
            lambda store: write_store(env, store).map(lambda _: obj)
        )

    def update(self, env: Env, obj: T) -> IOResult[T, str]:
        return read_store(env).bind_result(update(self.c, obj)).bind(
            lambda store: write_store(env, store).map(lambda _: obj)
        )

    def delete(self, env: Env, entity_id: int) -> IOResult[int, str]:
        return read_store(env).bind_result(delete(self.c, entity_id)).bind(
            lambda store: write_store(env, store).map(lambda _: entity_id)
        )

    def get(self, env: Env, entity_id: int) -> IOResult[T, str]:
        return read_store(env).bind_result(get(self.c, entity_id))

    def list(self, env: Env) -> IOResult[list[T], str]:
        return read_store(env).bind_result(lambda s: list_all(self.c, s))
