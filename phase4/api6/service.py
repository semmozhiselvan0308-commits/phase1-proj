import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATUS_FILE = Path(
    __import__("os").environ.get(
        "DE7_STATUS_PATH",
        PROJECT_ROOT / "phase3" / "de7" / "data" / "pipeline_status" / "pipeline_status.json",
    )
)
WAREHOUSE_PATH = Path(
    __import__("os").environ.get(
        "WAREHOUSE_PATH",
        PROJECT_ROOT / "phase3" / "de6" / "warehouse" / "network_analytics.db",
    )
)
GEOJSON_PATH = Path(
    __import__("os").environ.get(
        "GRID_GEOJSON_PATH",
        PROJECT_ROOT / "phase2" / "data" / "reference" / "milano-grid.geojson",
    )
)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read_pipeline_status(path: Path = STATUS_FILE) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _metric(status: dict[str, Any], name: str) -> int:
    metrics = status.get("metrics", {})
    if name in metrics:
        return int(metrics[name])

    processing = status.get("processing", {})
    return int(processing.get(name, 0))


def build_pipeline_status(status: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    as_of = _parse_datetime(
        status.get("AS_OF")
        or status.get("as_of")
        or status.get("analytics", {}).get("end_timestamp")
    )
    run_timestamp = _parse_datetime(status.get("run_timestamp"))
    if run_timestamp is None:
        raise ValueError("DE7 status record is missing run_timestamp.")

    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)

    age_timestamp = (
        as_of.replace(tzinfo=timezone.utc)
        if as_of is not None and as_of.tzinfo is None
        else as_of
    )
    analytics_age_seconds = (
        max(0.0, (current_time - age_timestamp).total_seconds())
        if age_timestamp is not None
        else None
    )
    tasks = {
        str(task): str(state)
        for task, state in status.get("tasks", {}).items()
    }
    reasons: list[str] = []
    if status.get("pipeline_result", "failure").lower() != "success":
        reasons.append("DE7 pipeline_result is not success.")
    for task, state in tasks.items():
        if state.lower() != "success":
            reasons.append(f"Task '{task}' has status '{state}'.")
    if as_of is None:
        reasons.append("The status record has no AS_OF value.")

    return {
        "run_id": str(status.get("run_id", "")),
        "run_timestamp": run_timestamp,
        "tasks": tasks,
        "rows_in": _metric(status, "rows_in"),
        "rows_rejected": _metric(status, "rows_rejected"),
        "nulls_handled": _metric(status, "nulls_handled"),
        "rows_published": _metric(status, "rows_published"),
        "AS_OF": as_of,
        "analytics_age_seconds": analytics_age_seconds,
        "analytics_freshness": (
            "unknown" if analytics_age_seconds is None
            else "fresh" if analytics_age_seconds <= 86400
            else "stale"
        ),
        "healthy": not reasons,
        "reasons": reasons,
    }


def _centroid(geometry: dict[str, Any]) -> tuple[float, float]:
    coordinates = geometry["coordinates"]
    if geometry["type"] == "MultiPolygon":
        coordinates = coordinates[0]
    ring = coordinates[0]
    longitudes = [point[0] for point in ring]
    latitudes = [point[1] for point in ring]
    return sum(latitudes) / len(latitudes), sum(longitudes) / len(longitudes)


def get_grid_location(grid_id: int) -> dict[str, Any] | None:
    connection = sqlite3.connect(WAREHOUSE_PATH)
    try:
        row = connection.execute(
            "SELECT grid_id, geometry_reference FROM dim_grid WHERE grid_id = ?",
            (grid_id,),
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None

    reference = str(row[1])
    match = re.search(r"cellId=(\d+)", reference)
    geojson_id = int(match.group(1)) if match else grid_id
    with GEOJSON_PATH.open("r", encoding="utf-8") as file:
        collection = json.load(file)

    feature = next(
        (
            item for item in collection["features"]
            if item.get("properties", {}).get("cellId") == geojson_id
            or str(item.get("id")) == str(geojson_id)
        ),
        None,
    )
    if feature is None:
        raise LookupError(f"No GeoJSON geometry found for grid '{grid_id}'.")

    latitude, longitude = _centroid(feature["geometry"])
    return {
        "grid_id": int(row[0]),
        "centroid_latitude": latitude,
        "centroid_longitude": longitude,
        "polygon_reference": reference,
    }
