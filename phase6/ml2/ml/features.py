import math
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WAREHOUSE_PATH = (
    PROJECT_ROOT / "phase3" / "de6" / "warehouse" / "network_analytics.db"
)
RECENT_WINDOW_HOURS = 24
BASELINE_WINDOW_HOURS = 24


FEATURE_COLUMNS = (
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _population_stddev(values: list[float], average: float) -> float:
    if not values:
        return 0.0
    return math.sqrt(sum((value - average) ** 2 for value in values) / len(values))


def calculate_feature_row(
    grid_id: int,
    feature_timestamp: str,
    recent_rows: list[tuple[float, float]],
    baseline_rows: list[tuple[float, float]],
) -> dict[str, float | int | str]:
    recent_activity = [row[0] for row in recent_rows]
    recent_internet = [row[1] for row in recent_rows]
    baseline_activity = [row[0] for row in baseline_rows]

    average = _mean(recent_activity)
    baseline_average = _mean(baseline_activity)
    growth = (
        (average - baseline_average) / baseline_average
        if baseline_average > 0
        else 1.0 if average > 0 else 0.0
    )
    total_activity = sum(recent_activity)
    internet_total = sum(recent_internet)

    return {
        "grid_id": grid_id,
        "feature_timestamp": feature_timestamp,
        "avg_activity": average,
        "activity_growth": growth,
        "active_hours": float(sum(value > 0 for value in recent_activity)),
        "peak_ratio": max(recent_activity, default=0.0) / average if average > 0 else 0.0,
        "variability": _population_stddev(recent_activity, average),
        "internet_share": internet_total / total_activity if total_activity > 0 else 0.0,
    }


def _read_hourly_rows(connection: sqlite3.Connection) -> dict[int, list[tuple[str, float, float]]]:
    rows = connection.execute(
        """
        SELECT
            dg.grid_id,
            dt.timestamp,
            COALESCE(f.total_activity, 0),
            COALESCE(f.internet_activity, 0)
        FROM dim_grid AS dg
        CROSS JOIN dim_time AS dt
        LEFT JOIN fact_network_activity AS f
            ON f.grid_key = dg.grid_key
           AND f.time_key = dt.time_key
        ORDER BY dg.grid_id, dt.timestamp
        """
    ).fetchall()

    by_grid: dict[int, list[tuple[str, float, float]]] = defaultdict(list)
    for grid_id, timestamp, total_activity, internet_activity in rows:
        by_grid[int(grid_id)].append(
            (str(timestamp), float(total_activity or 0), float(internet_activity or 0))
        )
    return by_grid


def build_feature_rows(
    hourly_rows: dict[int, list[tuple[str, float, float]]],
    recent_window_hours: int = RECENT_WINDOW_HOURS,
    baseline_window_hours: int = BASELINE_WINDOW_HOURS,
) -> list[dict[str, float | int | str]]:
    if recent_window_hours < 1 or baseline_window_hours < 1:
        raise ValueError("Feature windows must contain at least one hour.")

    rows: list[dict[str, float | int | str]] = []
    minimum_history = recent_window_hours + baseline_window_hours

    for grid_id, history in hourly_rows.items():
        for timestamp_index in range(minimum_history - 1, len(history)):
            baseline_start = timestamp_index - minimum_history + 1
            baseline_end = baseline_start + baseline_window_hours
            recent_end = timestamp_index + 1
            recent_start = recent_end - recent_window_hours
            baseline = [(row[1], row[2]) for row in history[baseline_start:baseline_end]]
            recent = [(row[1], row[2]) for row in history[recent_start:recent_end]]
            rows.append(
                calculate_feature_row(
                    grid_id=grid_id,
                    feature_timestamp=history[timestamp_index][0],
                    recent_rows=recent,
                    baseline_rows=baseline,
                )
            )
    return rows


def persist_feature_table(
    connection: sqlite3.Connection,
    feature_rows: Iterable[dict[str, float | int | str]],
) -> int:
    connection.execute("DROP TABLE IF EXISTS network_feature_table")
    connection.execute(
        """
        CREATE TABLE network_feature_table (
            grid_id INTEGER NOT NULL,
            feature_timestamp TEXT NOT NULL,
            avg_activity REAL NOT NULL,
            activity_growth REAL NOT NULL,
            active_hours REAL NOT NULL,
            peak_ratio REAL NOT NULL,
            variability REAL NOT NULL,
            internet_share REAL NOT NULL,
            PRIMARY KEY (grid_id, feature_timestamp)
        )
        """
    )
    values = [
        (
            row["grid_id"],
            row["feature_timestamp"],
            row["avg_activity"],
            row["activity_growth"],
            row["active_hours"],
            row["peak_ratio"],
            row["variability"],
            row["internet_share"],
        )
        for row in feature_rows
    ]
    connection.executemany(
        """
        INSERT INTO network_feature_table (
            grid_id, feature_timestamp, avg_activity, activity_growth,
            active_hours, peak_ratio, variability, internet_share
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    connection.commit()
    return len(values)


def build_and_persist_features(
    database_path: Path = DEFAULT_WAREHOUSE_PATH,
    recent_window_hours: int = RECENT_WINDOW_HOURS,
    baseline_window_hours: int = BASELINE_WINDOW_HOURS,
) -> int:
    with sqlite3.connect(database_path) as connection:
        hourly_rows = _read_hourly_rows(connection)
        feature_rows = build_feature_rows(
            hourly_rows,
            recent_window_hours=recent_window_hours,
            baseline_window_hours=baseline_window_hours,
        )
        return persist_feature_table(connection, feature_rows)


if __name__ == "__main__":
    count = build_and_persist_features()
    print(f"Persisted {count} rows to network_feature_table.")
