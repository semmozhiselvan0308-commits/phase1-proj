import csv
import json
import pickle
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_PATH = PROJECT_ROOT / "phase3" / "de6" / "warehouse" / "network_analytics.db"
ALERT_PATH = PROJECT_ROOT / "phase1" / "np1" / ".." / "np3" / "network_alerts.csv"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
FEATURE_NAMES = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


def load_feature_rows(database_path: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in connection.execute(
                """
                SELECT grid_id, feature_timestamp, avg_activity,
                       activity_growth, active_hours, peak_ratio,
                       variability, internet_share
                FROM network_feature_table
                ORDER BY feature_timestamp, grid_id
                """
            )
        ]


def load_hourly_activity(database_path: Path) -> dict[tuple[int, str], float]:
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            """
            SELECT dg.grid_id, dt.timestamp, COALESCE(f.total_activity, 0)
            FROM fact_network_activity AS f
            JOIN dim_time AS dt ON dt.time_key = f.time_key
            JOIN dim_grid AS dg ON dg.grid_key = f.grid_key
            """
        ).fetchall()
    return {(int(grid_id), str(timestamp)): float(activity) for grid_id, timestamp, activity in rows}


def assemble_labeled_rows(
    feature_rows: list[dict[str, Any]],
    hourly_activity: dict[tuple[int, str], float],
) -> list[dict[str, Any]]:
    labeled_rows = []
    for row in feature_rows:
        feature_time = datetime.fromisoformat(row["feature_timestamp"])
        target_time = feature_time + timedelta(hours=1)
        target_key = (int(row["grid_id"]), target_time.strftime("%Y-%m-%d %H:%M:%S"))
        if target_key not in hourly_activity:
            continue
        labeled_rows.append({
            **row,
            "target_timestamp": target_key[1],
            "future_activity": hourly_activity[target_key],
        })
    return labeled_rows


def chronological_split(rows: list[dict[str, Any]], train_fraction: float = 0.8):
    timestamps = sorted({row["feature_timestamp"] for row in rows})
    split_index = max(1, min(len(timestamps) - 1, int(len(timestamps) * train_fraction)))
    train_timestamps = set(timestamps[:split_index])
    test_timestamps = set(timestamps[split_index:])
    train_rows = [row for row in rows if row["feature_timestamp"] in train_timestamps]
    test_rows = [row for row in rows if row["feature_timestamp"] in test_timestamps]
    return train_rows, test_rows


def fit_training_thresholds(train_rows: list[dict[str, Any]]) -> dict[int, float]:
    values_by_grid: dict[int, list[float]] = defaultdict(list)
    for row in train_rows:
        values_by_grid[int(row["grid_id"])].append(float(row["future_activity"]))
    return {
        grid_id: float(np.percentile(values, 90))
        for grid_id, values in values_by_grid.items()
    }


def apply_proxy_labels(rows: list[dict[str, Any]], thresholds: dict[int, float]):
    return [
        {
            **row,
            "label": int(float(row["future_activity"]) > thresholds[int(row["grid_id"])]),
        }
        for row in rows
        if int(row["grid_id"]) in thresholds
    ]


def feature_matrix(rows: list[dict[str, Any]]):
    return np.array([[float(row[name]) for name in FEATURE_NAMES] for row in rows])


def class_balance(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    positives = sum(int(row["label"]) for row in rows)
    total = len(rows)
    return {
        "rows": total,
        "positive_rows": positives,
        "negative_rows": total - positives,
        "base_rate": positives / total if total else 0.0,
    }


def load_np3_alerts(path: Path) -> set[tuple[int, str]]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return {
            (int(row["grid_id"]), row["timestamp"])
            for row in csv.DictReader(file)
        }


def compare_with_alerts(test_rows, predictions, alert_keys):
    comparisons = []
    for row, prediction in zip(test_rows, predictions):
        key = (int(row["grid_id"]), row["target_timestamp"])
        model_positive = bool(prediction)
        alert_present = key in alert_keys
        comparisons.append({
            "grid_id": int(row["grid_id"]),
            "target_timestamp": row["target_timestamp"],
            "model_positive": model_positive,
            "np3_alert_present": alert_present,
            "agreement": model_positive == alert_present,
        })
    disagreements = [row for row in comparisons if not row["agreement"]]
    return {
        "compared_rows": len(comparisons),
        "np3_alert_rows_in_test": sum(row["np3_alert_present"] for row in comparisons),
        "agreements": len(comparisons) - len(disagreements),
        "disagreements": len(disagreements),
        "model_positive_np3_absent": sum(
            row["model_positive"] and not row["np3_alert_present"]
            for row in disagreements
        ),
        "model_negative_np3_present": sum(
            not row["model_positive"] and row["np3_alert_present"]
            for row in disagreements
        ),
        "examples": disagreements[:10],
    }


def train_and_evaluate(
    database_path: Path = WAREHOUSE_PATH,
    alert_path: Path = ALERT_PATH,
    output_dir: Path = OUTPUT_DIR,
):
    feature_rows = load_feature_rows(database_path)
    hourly_activity = load_hourly_activity(database_path)
    labeled_rows = assemble_labeled_rows(feature_rows, hourly_activity)
    train_rows, test_rows = chronological_split(labeled_rows)
    thresholds = fit_training_thresholds(train_rows)
    train_rows = apply_proxy_labels(train_rows, thresholds)
    test_rows = apply_proxy_labels(test_rows, thresholds)

    model = Pipeline([
        ("scale", StandardScaler()),
        ("classifier", LogisticRegression(class_weight="balanced", max_iter=1000, random_state=42)),
    ])
    x_train = feature_matrix(train_rows)
    x_test = feature_matrix(test_rows)
    y_train = np.array([row["label"] for row in train_rows])
    y_test = np.array([row["label"] for row in test_rows])
    model.fit(x_train, y_train)
    predictions = model.predict(x_test)

    classifier = model.named_steps["classifier"]
    coefficients = {
        name: float(value)
        for name, value in zip(FEATURE_NAMES, classifier.coef_[0])
    }
    alert_comparison = compare_with_alerts(test_rows, predictions, load_np3_alerts(alert_path))
    report = {
        "algorithm": "StandardScaler + class-balanced LogisticRegression",
        "feature_names": FEATURE_NAMES,
        "label": "future total_activity > grid-specific training-period 90th percentile",
        "split": {
            "strategy": "chronological 80/20 split by unique feature_timestamp",
            "train_earliest": train_rows[0]["feature_timestamp"],
            "train_latest": train_rows[-1]["feature_timestamp"],
            "test_earliest": test_rows[0]["feature_timestamp"],
            "test_latest": test_rows[-1]["feature_timestamp"],
        },
        "metrics": {
            "accuracy": float(accuracy_score(y_test, predictions)),
            "precision": float(precision_score(y_test, predictions, zero_division=0)),
            "recall": float(recall_score(y_test, predictions, zero_division=0)),
        },
        "class_balance": {
            "train": class_balance(train_rows),
            "test": class_balance(test_rows),
        },
        "coefficients_on_standardized_features": coefficients,
        "training_thresholds": {
            "method": "grid-specific 90th percentile fitted on train rows only",
            "grid_count": len(thresholds),
        },
        "np3_comparison": alert_comparison,
        "observations": [
            "The chronological split tests future timestamps rather than randomly mixing neighboring hours.",
            "The standardized coefficients expose which trailing-window signals move the investigation-risk estimate up or down, but they do not establish causation.",
            f"The class-balanced baseline produced {int(sum(predictions))} positive test predictions against a test base rate of {class_balance(test_rows)['base_rate']:.3f}; precision and recall must be read with that imbalance in mind.",
            f"NP3 contributed {alert_comparison['np3_alert_rows_in_test']} alert rows in the test period, so any comparison limitation is reported rather than treated as model agreement.",
            "The proxy label and NP3 rules answer different questions, so disagreements require an operator to inspect the grid rather than treating either output as a confirmed fault.",
        ],
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "risk_classifier.pkl").open("wb") as file:
        pickle.dump(model, file)
    (output_dir / "evaluation_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    with (output_dir / "test_predictions.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["grid_id", "feature_timestamp", "target_timestamp", "label", "prediction", "future_activity"])
        writer.writeheader()
        writer.writerows({
            "grid_id": row["grid_id"],
            "feature_timestamp": row["feature_timestamp"],
            "target_timestamp": row["target_timestamp"],
            "label": row["label"],
            "prediction": int(prediction),
            "future_activity": row["future_activity"],
        } for row, prediction in zip(test_rows, predictions))
    return report


if __name__ == "__main__":
    result = train_and_evaluate()
    print(json.dumps(result, indent=2))
