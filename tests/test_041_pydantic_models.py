"""Tests for flow 041 -- Pydantic Models."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_041",
    Path(__file__).resolve().parent.parent / "flows" / "041_pydantic_models.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_041"] = _mod
_spec.loader.exec_module(_mod)

PipelineConfig = _mod.PipelineConfig
UserRecord = _mod.UserRecord
ProcessingResult = _mod.ProcessingResult
extract_users = _mod.extract_users
validate_users = _mod.validate_users
summarize = _mod.summarize
pydantic_models_flow = _mod.pydantic_models_flow


def test_pipeline_config_defaults() -> None:
    config = PipelineConfig()
    assert config.source == "users_api"
    assert config.batch_size == 100
    assert config.enable_validation is True


def test_user_record_creation() -> None:
    user = UserRecord(name="Alice", email="alice@example.com", age=30)
    assert user.name == "Alice"
    assert user.email == "alice@example.com"
    assert user.age == 30


def test_extract_users() -> None:
    config = PipelineConfig(source="test", batch_size=2)
    result = extract_users.fn(config)
    assert isinstance(result, list)
    assert len(result) == 2
    assert isinstance(result[0], UserRecord)


def test_validate_users() -> None:
    users = [
        UserRecord(name="Alice", email="a@b.com", age=30),
        UserRecord(name="Bob", email="b@b.com", age=25),
    ]
    result = validate_users.fn(users)
    assert isinstance(result, ProcessingResult)
    assert len(result.records) == 2
    assert len(result.errors) == 0


def test_summarize() -> None:
    result = ProcessingResult(records=[{"name": "Alice"}], errors=[], summary="1 valid, 0 errors")
    msg = summarize.fn(result)
    assert isinstance(msg, str)
    assert "1 valid" in msg


def test_flow_runs() -> None:
    state = pydantic_models_flow(return_state=True)
    assert state.is_completed()
