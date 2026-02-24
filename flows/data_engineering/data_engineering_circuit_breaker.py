"""Circuit Breaker.

Circuit breaker state machine: closed -> open -> half_open -> closed.
After N consecutive failures the circuit opens, preventing further calls
until a recovery probe succeeds.

Airflow equivalent: None (Prefect-native resilience pattern).
Prefect approach:    State machine in a Pydantic model, deterministic
                     simulation via boolean outcomes list.
"""

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CircuitState(BaseModel):
    """State of a circuit breaker."""

    name: str
    state: str = "closed"  # closed, open, half_open
    consecutive_failures: int = 0
    failure_threshold: int = 3
    total_calls: int = 0
    total_failures: int = 0
    total_successes: int = 0
    trips: int = 0


class CallResult(BaseModel):
    """Result of a single call through the circuit breaker."""

    call_number: int
    allowed: bool
    success: bool | None = None
    circuit_state: str


class CircuitBreakerResult(BaseModel):
    """Summary of circuit breaker simulation."""

    total_calls: int
    successful_calls: int
    failed_calls: int
    rejected_calls: int
    circuit_trips: int
    final_state: str
    call_log: list[CallResult]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def create_circuit(name: str, threshold: int = 3) -> CircuitState:
    """Create a new circuit breaker.

    Args:
        name: Circuit breaker name.
        threshold: Number of consecutive failures to trip.

    Returns:
        Initial CircuitState.
    """
    print(f"Created circuit breaker '{name}' with threshold={threshold}")
    return CircuitState(name=name, failure_threshold=threshold)


@task
def call_with_circuit(circuit: CircuitState, should_succeed: bool) -> tuple[CircuitState, CallResult]:
    """Execute a call through the circuit breaker.

    Args:
        circuit: Current circuit state.
        should_succeed: Whether this call should succeed.

    Returns:
        Tuple of (updated CircuitState, CallResult).
    """
    call_num = circuit.total_calls + 1

    # If open, reject the call but try half_open probe
    if circuit.state == "open":
        # Transition to half_open for a probe
        circuit = circuit.model_copy(update={"state": "half_open"})
        print(f"Call {call_num}: circuit half_open, probing...")

    if circuit.state == "half_open":
        circuit = circuit.model_copy(update={"total_calls": call_num})
        if should_succeed:
            # Recovery: close the circuit
            circuit = circuit.model_copy(
                update={
                    "state": "closed",
                    "consecutive_failures": 0,
                    "total_successes": circuit.total_successes + 1,
                }
            )
            print(f"Call {call_num}: probe SUCCESS, circuit CLOSED")
            return circuit, CallResult(call_number=call_num, allowed=True, success=True, circuit_state="closed")
        else:
            # Probe failed: re-open
            circuit = circuit.model_copy(
                update={
                    "state": "open",
                    "total_failures": circuit.total_failures + 1,
                    "consecutive_failures": circuit.consecutive_failures + 1,
                }
            )
            print(f"Call {call_num}: probe FAILED, circuit OPEN")
            return circuit, CallResult(call_number=call_num, allowed=True, success=False, circuit_state="open")

    # Closed state: execute normally
    circuit = circuit.model_copy(update={"total_calls": call_num})
    if should_succeed:
        circuit = circuit.model_copy(
            update={
                "consecutive_failures": 0,
                "total_successes": circuit.total_successes + 1,
            }
        )
        return circuit, CallResult(call_number=call_num, allowed=True, success=True, circuit_state="closed")
    else:
        new_failures = circuit.consecutive_failures + 1
        updates: dict = {
            "consecutive_failures": new_failures,
            "total_failures": circuit.total_failures + 1,
        }
        if new_failures >= circuit.failure_threshold:
            updates["state"] = "open"
            updates["trips"] = circuit.trips + 1
            print(f"Call {call_num}: FAILED, circuit TRIPPED (open)")
        else:
            print(f"Call {call_num}: FAILED ({new_failures}/{circuit.failure_threshold})")
        circuit = circuit.model_copy(update=updates)
        return circuit, CallResult(
            call_number=call_num,
            allowed=True,
            success=False,
            circuit_state=circuit.state,
        )


@task
def simulate_service_calls(circuit: CircuitState, outcomes: list[bool]) -> CircuitBreakerResult:
    """Run a series of calls through the circuit breaker.

    Args:
        circuit: Initial circuit state.
        outcomes: List of booleans (True=succeed, False=fail).

    Returns:
        CircuitBreakerResult with full call log.
    """
    call_log: list[CallResult] = []
    for should_succeed in outcomes:
        circuit, result = call_with_circuit.fn(circuit, should_succeed)
        call_log.append(result)

    return CircuitBreakerResult(
        total_calls=len(call_log),
        successful_calls=sum(1 for r in call_log if r.success is True),
        failed_calls=sum(1 for r in call_log if r.success is False),
        rejected_calls=sum(1 for r in call_log if not r.allowed),
        circuit_trips=circuit.trips,
        final_state=circuit.state,
        call_log=call_log,
    )


@task
def circuit_breaker_report(result: CircuitBreakerResult) -> str:
    """Generate a summary report.

    Args:
        result: Simulation result.

    Returns:
        Summary string.
    """
    return (
        f"Circuit breaker: {result.total_calls} calls, "
        f"{result.successful_calls} success, {result.failed_calls} failed, "
        f"{result.circuit_trips} trips, final state: {result.final_state}"
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_circuit_breaker", log_prints=True)
def circuit_breaker_flow(
    outcomes: list[bool] | None = None,
    threshold: int = 3,
) -> CircuitBreakerResult:
    """Demonstrate circuit breaker pattern with simulated calls.

    Args:
        outcomes: List of success/fail outcomes. Uses default if None.
        threshold: Failure threshold to trip circuit.

    Returns:
        CircuitBreakerResult.
    """
    if outcomes is None:
        # Simulate: success, success, then 3 failures (trip), then recovery attempt
        outcomes = [True, True, False, False, False, True, True, False, True, True]

    circuit = create_circuit("service_api", threshold=threshold)
    result = simulate_service_calls(circuit, outcomes)
    report = circuit_breaker_report(result)
    print(report)
    return result


if __name__ == "__main__":
    circuit_breaker_flow()
