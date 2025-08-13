# dalapy

Small data-layer helpers built on the [returns](https://github.com/dry-python/returns)
library and [Pydantic v2](https://docs.pydantic.dev/).  It provides a generic
repository that persists Pydantic dataclasses to disk.  Each entity dataclass
lives in its own module making it simple to extend the domain.

## Development

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[test]
```

Run tests with coverage:

```bash
pytest
```
