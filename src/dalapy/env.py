from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass

from tinydb import TinyDB
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware

from returns.io import IOResult, IOSuccess, IOFailure
from returns.context import RequiresContextIOResult as RCIOResult


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
            env.db.close()
            env.db = None
        return IOSuccess(env)
    except Exception as exc:
        return IOFailure(f'db_close_error: {exc}')


def ensure_env() -> RCIOResult[Env, str, Env]:
    return RCIOResult(_open_db)


def shutdown_env() -> RCIOResult[Env, str, Env]:
    return RCIOResult(_close_db)
