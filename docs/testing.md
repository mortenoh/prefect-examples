# Testing

How to test Prefect flows and tasks. All examples in this project have
corresponding tests in the `tests/` directory.

## Running tests

```bash
make test
# or directly:
uv run pytest
```

## Testing tasks with `.fn()`

Call `.fn()` on a task to execute the underlying Python function without the
Prefect runtime. This makes unit tests fast and free of side effects:

```python
from my_flow import greet, compute_sum

def test_greet():
    result = greet.fn("World")
    assert result == "Hello, World!"

def test_compute_sum():
    result = compute_sum.fn(3, 7)
    assert result == 10
```

`.fn()` bypasses retries, state tracking, and logging -- you are testing pure
business logic.

## Testing flows with `return_state=True`

Pass `return_state=True` to a flow call to get the final `State` object
instead of the return value. This lets you assert on completion status:

```python
def test_flow_completes():
    state = my_flow(return_state=True)
    assert state.is_completed()
```

You can also check for failure:

```python
def test_flow_fails_on_bad_input():
    state = my_flow(bad_param=True, return_state=True)
    assert state.is_failed()
```

## Importing digit-prefixed modules with importlib

Flow files like `001_hello_world.py` start with a digit, which makes them
invalid as Python module names. Use `importlib` to import them:

```python
import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_001",
    Path(__file__).resolve().parent.parent / "flows" / "001_hello_world.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_001"] = _mod
_spec.loader.exec_module(_mod)

say_hello = _mod.say_hello
hello_world = _mod.hello_world
```

This pattern is used in every test file in the project.

## The `flow_module` fixture

The shared `conftest.py` provides a `flow_module` fixture that wraps the
importlib boilerplate:

```python
# tests/conftest.py
@pytest.fixture
def flow_module() -> type:
    class _Loader:
        @staticmethod
        def __call__(name: str) -> ModuleType:
            path = Path(__file__).resolve().parent.parent / "flows" / f"{name}.py"
            spec = importlib.util.spec_from_file_location(name, path)
            assert spec and spec.loader
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            spec.loader.exec_module(mod)
            return mod
    return _Loader
```

Use it in tests:

```python
def test_etl(flow_module):
    mod = flow_module("004_taskflow_etl")
    result = mod.extract.fn()
    assert isinstance(result, dict)
```

## Typical test structure

A complete test file for a flow:

```python
"""Tests for flow 004 — Taskflow / ETL."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_004",
    Path(__file__).resolve().parent.parent / "flows" / "004_taskflow_etl.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_004"] = _mod
_spec.loader.exec_module(_mod)

extract = _mod.extract
transform = _mod.transform
load = _mod.load
taskflow_etl_flow = _mod.taskflow_etl_flow


def test_extract_returns_dict() -> None:
    result = extract.fn()
    assert isinstance(result, dict)


def test_transform() -> None:
    raw = extract.fn()
    result = transform.fn(raw)
    assert "users" in result


def test_flow_runs() -> None:
    state = taskflow_etl_flow(return_state=True)
    assert state.is_completed()
```

Key takeaways:

1. **`.fn()`** for unit-testing individual tasks (fast, no Prefect overhead).
2. **`return_state=True`** for integration-testing the full flow.
3. **`importlib`** to handle digit-prefixed filenames.
4. **`conftest.py`** fixture to reduce import boilerplate.
