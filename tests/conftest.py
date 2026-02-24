"""Shared test fixtures."""

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def flow_module() -> type:
    """Factory fixture that imports a flow file by its filename.

    Usage::

        def test_something(flow_module):
            mod = flow_module("001_hello_world")
            mod.hello_world()
    """

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

    return _Loader  # type: ignore[return-value]
