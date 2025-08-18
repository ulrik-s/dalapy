"""Repository factories for the example project models."""

from __future__ import annotations

from pydantic import TypeAdapter

from dalapy import UniqueRule, RepoSpec, repo_factory

from .models import CSModuleGroup, CSModule, System


CSMODULE_SPEC = RepoSpec[CSModule](
    table_name="cs_modules",
    adapter=TypeAdapter(CSModule),
    unique=(UniqueRule("sku", nocase=False),),
)

SYSTEM_SPEC = RepoSpec[System](
    table_name="systems",
    adapter=TypeAdapter(System),
    unique=(UniqueRule("name", nocase=True),),
)

CSMODULE_GROUP_SPEC = RepoSpec[CSModuleGroup](
    table_name="cs_module_groups",
    adapter=TypeAdapter(CSModuleGroup),
    unique=(UniqueRule("tag", nocase=False),),
)

CSModules = repo_factory(CSMODULE_SPEC)
Systems = repo_factory(SYSTEM_SPEC)
CSModuleGroups = repo_factory(CSMODULE_GROUP_SPEC)

__all__ = [
    "CSMODULE_SPEC",
    "CSMODULE_GROUP_SPEC",
    "SYSTEM_SPEC",

    "CSModuleGroups",
    "CSModules",
    "Systems"
]
