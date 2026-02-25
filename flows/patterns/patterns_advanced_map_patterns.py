"""Advanced Map Patterns.

Advanced .map() usage: list-of-dicts unpacking, chained maps, combining results.

Airflow equivalent: expand_kwargs, partial+expand (DAG 054).
Prefect approach:    .map() with list unpacking and result collection.
"""

from typing import Any

from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def process_station(station_id: str, lat: float, lon: float) -> dict[str, Any]:
    """Process weather data for a single station.

    Args:
        station_id: The station identifier.
        lat: Latitude of the station.
        lon: Longitude of the station.

    Returns:
        A dict with station processing results.
    """
    reading = round((lat + lon) * 0.1, 2)
    result = {"station_id": station_id, "lat": lat, "lon": lon, "reading": reading}
    print(f"Processed station {station_id}: reading={reading}")
    return result


@task
def extract_data(date: str, variable: str) -> dict[str, Any]:
    """Extract data for a given date and variable.

    Args:
        date: The date string for extraction.
        variable: The variable name to extract.

    Returns:
        A dict with the extracted data.
    """
    result = {"date": date, "variable": variable, "value": len(date) + len(variable)}
    print(f"Extracted {variable} for {date}")
    return result


@task
def summarize_results(results: list[dict[str, Any]]) -> str:
    """Summarize a list of processed results.

    Args:
        results: A list of result dicts.

    Returns:
        A summary string.
    """
    summary = f"Processed {len(results)} items"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_advanced_map_patterns", log_prints=True)
def advanced_map_patterns_flow() -> None:
    """Demonstrate advanced .map() patterns."""
    # Pattern 1: Unpack list-of-dicts into parallel .map() calls
    stations = [
        {"station_id": "WX001", "lat": 40.7, "lon": -74.0},
        {"station_id": "WX002", "lat": 34.1, "lon": -118.2},
        {"station_id": "WX003", "lat": 41.9, "lon": -87.6},
    ]
    station_futures = process_station.map(
        [s["station_id"] for s in stations],
        [s["lat"] for s in stations],
        [s["lon"] for s in stations],
    )
    station_results = [f.result() for f in station_futures]
    summarize_results(station_results)

    # Pattern 2: Chained maps with different parameters
    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    variables = ["temperature", "humidity", "pressure"]
    extract_futures = extract_data.map(dates, variables)
    extract_results = [f.result() for f in extract_futures]
    summarize_results(extract_results)


if __name__ == "__main__":
    advanced_map_patterns_flow()
