# dalapy

Small data-layer helpers built on the [returns](https://github.com/dry-python/returns)
library and [Pydantic v2](https://docs.pydantic.dev/).  It provides a generic
repository that persists Pydantic dataclasses to disk.  Each entity dataclass
lives in its own module making it simple to extend the domain.

## Development

Install dependencies with [uv](https://github.com/astral-sh/uv):

```bash
uv sync --extra test
```

Run tests and display coverage statistics:

```bash
uv run pytest
```
