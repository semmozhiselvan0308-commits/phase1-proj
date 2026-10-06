import sqlite3
from pathlib import Path

import pytest

from ml.features import build_feature_rows, calculate_feature_row, persist_feature_table


def test_features_use_only_data_through_feature_timestamp():
    history = {
        4821: [
            (f"2024-01-01T{hour:02d}:00:00", float(hour + 1), float(hour + 1) / 2)
            for hour in range(48)
        ]
    }

    baseline_rows = build_feature_rows(history)
    target = baseline_rows[0]

    history_with_future_data = {
        4821: history[4821]
        + [
            ("2024-01-03T00:00:00", 999999.0, 999999.0),
            ("2024-01-03T01:00:00", 999999.0, 999999.0),
        ]
    }
    recomputed = build_feature_rows(history_with_future_data)
    target_with_future_data = next(
        row
        for row in recomputed
        if row["feature_timestamp"] == target["feature_timestamp"]
    )

    assert target_with_future_data == target


def test_deliberately_leaky_feature_changes_when_future_data_is_added():
    recent = [(float(value), float(value) / 2) for value in range(1, 25)]
    baseline = [(1.0, 0.5)] * 24
    clean = calculate_feature_row(4821, "2024-01-02T23:00:00", recent, baseline)

    leaky_recent = recent + [(10000.0, 5000.0)]
    intentionally_broken = calculate_feature_row(
        4821,
        "2024-01-02T23:00:00",
        leaky_recent,
        baseline,
    )

    with pytest.raises(AssertionError):
        assert intentionally_broken == clean


def test_hand_check_average_and_peak_ratio():
    recent = [(float(value), 0.0) for value in [1, 2, 3, 4]]
    baseline = [(1.0, 0.0)] * 4

    row = calculate_feature_row(4821, "2024-01-01T03:00:00", recent, baseline)

    assert row["avg_activity"] == pytest.approx(2.5)
    assert row["peak_ratio"] == pytest.approx(1.6)


def test_zero_activity_has_finite_features():
    zero_rows = [(0.0, 0.0)] * 4
    row = calculate_feature_row(4821, "2024-01-01T03:00:00", zero_rows, zero_rows)

    assert row["activity_growth"] == 0.0
    assert row["peak_ratio"] == 0.0
    assert row["internet_share"] == 0.0
    assert all(value == value for value in row.values() if isinstance(value, float))


def test_persists_required_feature_columns(tmp_path: Path):
    database_path = tmp_path / "features.db"
    connection = sqlite3.connect(database_path)
    try:
        count = persist_feature_table(
            connection,
            [
                {
                    "grid_id": 4821,
                    "feature_timestamp": "2024-01-01T23:00:00",
                    "avg_activity": 2.5,
                    "activity_growth": 1.5,
                    "active_hours": 4.0,
                    "peak_ratio": 1.6,
                    "variability": 1.118,
                    "internet_share": 0.0,
                }
            ],
        )
        columns = [row[1] for row in connection.execute("PRAGMA table_info(network_feature_table)")]
    finally:
        connection.close()

    assert count == 1
    assert columns == ["grid_id", "feature_timestamp", "avg_activity", "activity_growth", "active_hours", "peak_ratio", "variability", "internet_share"]
