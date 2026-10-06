import sqlite3

import pandas as pd
import pytest

from phase6.ml6.batch_score import (
    FEATURE_NAMES,
    RISK_SCORE_COLUMNS,
    calculate_risk_scores,
    load_model,
    load_feature_table,
    persist_risk_scores,
)


def test_feature_order_is_exact_ml2_order():
    assert FEATURE_NAMES == (
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share",
    )


def test_real_model_loads():
    model, model_version = load_model()

    assert hasattr(model, "predict_proba")
    assert model_version.startswith("ml3-")
    assert len(model_version) > 10


def test_risk_scores_have_valid_shape_and_range():
    model, model_version = load_model()

    features = pd.DataFrame(
        [
            {
                "grid_id": 48,
                "feature_timestamp": "2013-11-07 18:00:00",
                "avg_activity": 100.0,
                "activity_growth": 0.10,
                "active_hours": 20.0,
                "peak_ratio": 1.50,
                "variability": 0.20,
                "internet_share": 0.70,
            },
            {
                "grid_id": 147,
                "feature_timestamp": "2013-11-07 18:00:00",
                "avg_activity": 500.0,
                "activity_growth": 0.80,
                "active_hours": 24.0,
                "peak_ratio": 3.00,
                "variability": 1.00,
                "internet_share": 0.50,
            },
        ]
    )

    scores = calculate_risk_scores(
        features,
        model,
        model_version,
    )

    assert list(scores.columns) == list(RISK_SCORE_COLUMNS)
    assert len(scores) == 2
    assert scores["risk_score"].between(0.0, 1.0).all()
    assert scores["risk_level"].isin(
        ["LOW", "MEDIUM", "HIGH"]
    ).all()
    assert (scores["model_version"] == model_version).all()


def test_persistence_is_rerun_safe(tmp_path):
    database_path = tmp_path / "test.db"

    scores = pd.DataFrame(
        [
            {
                "grid_id": 48,
                "feature_timestamp": "2013-11-07 18:00:00",
                "risk_score": 0.80,
                "risk_level": "HIGH",
                "model_version": "test-model",
            },
            {
                "grid_id": 147,
                "feature_timestamp": "2013-11-07 18:00:00",
                "risk_score": 0.30,
                "risk_level": "LOW",
                "model_version": "test-model",
            },
        ]
    )

    first = persist_risk_scores(
        scores,
        database_path,
    )

    second = persist_risk_scores(
        scores,
        database_path,
    )

    assert first == 2
    assert second == 2

    with sqlite3.connect(database_path) as connection:
        row_count = connection.execute(
            "SELECT COUNT(*) FROM network_risk_scores"
        ).fetchone()[0]

        unique_keys = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT grid_id, feature_timestamp
                FROM network_risk_scores
            )
            """
        ).fetchone()[0]

    assert row_count == 2
    assert unique_keys == 2


def test_feature_table_is_not_empty():
    features = load_feature_table()

    assert not features.empty
    assert list(features.columns) == [
        "grid_id",
        "feature_timestamp",
        *FEATURE_NAMES,
    ]
