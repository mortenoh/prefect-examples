"""Shared test fixtures."""

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def flow_module() -> type:
    """Factory fixture that imports a flow file by group and name.

    Usage::

        def test_something(flow_module):
            mod = flow_module("basics", "basics_hello_world")
            mod.hello_world()
    """

    class _Loader:
        @staticmethod
        def __call__(group: str, name: str) -> ModuleType:
            path = PROJECT_ROOT / "flows" / group / f"{name}.py"
            spec = importlib.util.spec_from_file_location(name, path)
            assert spec and spec.loader
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            spec.loader.exec_module(mod)
            return mod

    return _Loader
