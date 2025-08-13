"""Configuration for storage locations."""

from __future__ import annotations

from pathlib import Path
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass


@pyd_dataclass(config=ConfigDict(extra="ignore"))
class Env:
    """Runtime environment defining where data is stored."""

    data_path: Path
    lock_path: Path | None = None  # enable later for multiprocess
