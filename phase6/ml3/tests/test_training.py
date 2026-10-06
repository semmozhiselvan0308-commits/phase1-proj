from datetime import datetime

import pytest

from train_model import chronological_split, fit_training_thresholds, apply_proxy_labels


def make_rows():
    return [
        {
            "grid_id": 4821,
            "feature_timestamp": timestamp,
            "future_activity": float(index),
        }
        for index, timestamp in enumerate(
            [
                "2013-11-01 00:00:00",
                "2013-11-01 01:00:00",
                "2013-11-01 02:00:00",
                "2013-11-01 03:00:00",
                "2013-11-01 04:00:00",
            ]
        )
    ]


def test_chronological_split_has_non_overlapping_ranges():
    train, test = chronological_split(make_rows(), train_fraction=0.6)

    assert max(row["feature_timestamp"] for row in train) < min(row["feature_timestamp"] for row in test)
    assert train[0]["feature_timestamp"] == "2013-11-01 00:00:00"
    assert test[-1]["feature_timestamp"] == "2013-11-01 04:00:00"


def test_proxy_threshold_is_fitted_on_training_rows():
    rows = make_rows()
    thresholds = fit_training_thresholds(rows[:3])
    labeled = apply_proxy_labels(rows[3:], thresholds)

    assert thresholds[4821] == pytest.approx(1.8)
    assert [row["label"] for row in labeled] == [1, 1]
