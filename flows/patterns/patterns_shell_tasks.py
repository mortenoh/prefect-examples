"""Shell Tasks.

Run shell commands and scripts from Prefect tasks using subprocess.

Airflow equivalent: BashOperator (DAGs 043-046).
Prefect approach:    subprocess.run() inside a @task -- no special operator.
"""

import subprocess
from typing import Any

from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def run_command(cmd: str) -> str:
    """Run a shell command and return its stdout.

    Args:
        cmd: The shell command string to execute.

    Returns:
        The stripped stdout output.
    """
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
    output = result.stdout.strip()
    print(f"$ {cmd}")
    print(output)
    return output


@task
def run_script(script: str) -> str:
    """Run a multi-line shell script and return its stdout.

    Args:
        script: The shell script content to execute.

    Returns:
        The stripped stdout output.
    """
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True)
    output = result.stdout.strip()
    print(f"Script output:\n{output}")
    return output


@task
def capture_output(cmd: str) -> dict[str, Any]:
    """Run a command and capture stdout, stderr, and return code.

    Args:
        cmd: The shell command string to execute.

    Returns:
        A dict with stdout, stderr, and returncode.
    """
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    output = {
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
        "returncode": result.returncode,
    }
    print(f"$ {cmd} -> returncode={output['returncode']}")
    return output


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_shell_tasks", log_prints=True)
def shell_tasks_flow() -> None:
    """Run system commands and capture their output."""
    run_command("echo 'Hello from shell'")
    run_script("echo 'line 1'\necho 'line 2'\necho 'line 3'")
    capture_output("echo 'captured output'")


if __name__ == "__main__":
    shell_tasks_flow()
