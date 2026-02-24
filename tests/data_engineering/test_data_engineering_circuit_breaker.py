"""Tests for flow 075 -- Circuit Breaker."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_circuit_breaker",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_circuit_breaker.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_circuit_breaker"] = _mod
_spec.loader.exec_module(_mod)

CircuitState = _mod.CircuitState
CallResult = _mod.CallResult
CircuitBreakerResult = _mod.CircuitBreakerResult
create_circuit = _mod.create_circuit
call_with_circuit = _mod.call_with_circuit
simulate_service_calls = _mod.simulate_service_calls
circuit_breaker_report = _mod.circuit_breaker_report
circuit_breaker_flow = _mod.circuit_breaker_flow


def test_circuit_state_defaults() -> None:
    c = CircuitState(name="test")
    assert c.state == "closed"
    assert c.consecutive_failures == 0


def test_create_circuit() -> None:
    circuit = create_circuit.fn("test", threshold=5)
    assert circuit.failure_threshold == 5


def test_successful_call() -> None:
    circuit = CircuitState(name="test")
    updated, result = call_with_circuit.fn(circuit, True)
    assert result.success is True
    assert updated.state == "closed"


def test_circuit_trips_after_threshold() -> None:
    circuit = CircuitState(name="test", failure_threshold=2)
    circuit, _ = call_with_circuit.fn(circuit, False)
    assert circuit.state == "closed"
    circuit, _ = call_with_circuit.fn(circuit, False)
    assert circuit.state == "open"
    assert circuit.trips == 1


def test_circuit_recovery() -> None:
    circuit = CircuitState(name="test", state="open", consecutive_failures=3, trips=1, failure_threshold=3)
    circuit, result = call_with_circuit.fn(circuit, True)
    assert circuit.state == "closed"
    assert result.success is True


def test_simulate_mixed_outcomes() -> None:
    circuit = CircuitState(name="test", failure_threshold=2)
    result = simulate_service_calls.fn(circuit, [True, False, False, True, True])
    assert result.circuit_trips >= 1
    assert result.successful_calls >= 1


def test_flow_runs() -> None:
    state = circuit_breaker_flow(return_state=True)
    assert state.is_completed()
