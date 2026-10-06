import csv
import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from phase1.np3.network_alert_generator import calculate_bucket_baseline


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_PATH = PROJECT_ROOT / "phase3" / "de6" / "warehouse" / "network_analytics.db"
NP3_ALERT_PATH = PROJECT_ROOT / "phase1" / "np3" / "network_alerts.csv"
ML3_PREDICTIONS_PATH = PROJECT_ROOT / "phase6" / "ml3" / "outputs" / "test_predictions.csv"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
ANOMALY_THRESHOLD_PERCENT = 50.0


def load_hourly_activity(database_path: Path = WAREHOUSE_PATH) -> pd.DataFrame:
    with sqlite3.connect(database_path) as connection:
        return pd.read_sql_query(
            """
            SELECT dg.grid_id, dt.timestamp, COALESCE(f.total_activity, 0) AS total_activity
            FROM fact_network_activity AS f
            JOIN dim_time AS dt ON dt.time_key = f.time_key
            JOIN dim_grid AS dg ON dg.grid_key = f.grid_key
            ORDER BY dg.grid_id, dt.timestamp
            """,
            connection,
            parse_dates=["timestamp"],
        )


def calculate_anomaly_scores(
    hourly_activity: pd.DataFrame,
    threshold_percent: float = ANOMALY_THRESHOLD_PERCENT,
) -> pd.DataFrame:
    if hourly_activity.empty:
        raise ValueError("Hourly activity data is empty.")

    scores = hourly_activity.copy()
    scores["hour_of_day"] = scores["timestamp"].dt.hour
    scores = calculate_bucket_baseline(
        scores,
        bucket_key="hour_of_day",
        reducer="median",
        exclude_current=True,
    )
    bucket_counts = scores.groupby(["grid_id", "hour_of_day"])["total_activity"].transform("count") - 1
    scores["baseline_bucket_count"] = bucket_counts.astype(int)
    scores["baseline_activity"] = scores["baseline_activity"].fillna(0.0)

    baseline = scores["baseline_activity"].to_numpy(dtype=float)
    current = scores["total_activity"].to_numpy(dtype=float)
    scores["deviation_percent"] = np.where(
        baseline > 0,
        (current - baseline) / baseline * 100.0,
        np.where(current == 0, 0.0, 100.0),
    )
    scores["anomaly_score"] = scores["deviation_percent"].abs()
    scores["direction"] = np.select(
        [scores["deviation_percent"] > 0, scores["deviation_percent"] < 0],
        ["HIGH", "LOW"],
        default="NORMAL",
    )
    scores["anomaly_flag"] = scores["anomaly_score"] >= threshold_percent
    scores["reason"] = "Within the configured historical-deviation band."
    flagged = scores["anomaly_flag"]
    scores.loc[flagged, "reason"] = (
        scores.loc[flagged, "direction"]
        + " anomaly: current activity "
        + scores.loc[flagged, "total_activity"].map(lambda value: f"{value:.2f}")
        + " vs hour-"
        + scores.loc[flagged, "hour_of_day"].map(lambda value: f"{int(value):02d}")
        + " historical median "
        + scores.loc[flagged, "baseline_activity"].map(lambda value: f"{value:.2f}")
        + " ("
        + scores.loc[flagged, "deviation_percent"].map(lambda value: f"{value:+.1f}")
        + "%)."
    )
    return scores[
        [
            "grid_id",
            "timestamp",
            "hour_of_day",
            "total_activity",
            "baseline_activity",
            "baseline_bucket_count",
            "deviation_percent",
            "anomaly_score",
            "direction",
            "anomaly_flag",
            "reason",
        ]
    ]


def persist_anomaly_scores(
    scores: pd.DataFrame,
    database_path: Path = WAREHOUSE_PATH,
) -> int:
    with sqlite3.connect(database_path) as connection:
        scores.to_sql("network_anomaly_scores", connection, if_exists="replace", index=False)
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_anomaly_grid_time ON network_anomaly_scores (grid_id, timestamp)"
        )
        connection.commit()
    return len(scores)


def _np3_alert_keys(alert_path: Path) -> set[tuple[int, str]]:
    if not alert_path.exists():
        return set()
    with alert_path.open("r", encoding="utf-8-sig", newline="") as file:
        return {(int(row["grid_id"]), row["timestamp"]) for row in csv.DictReader(file)}


def compare_three_mechanisms(
    scores: pd.DataFrame,
    predictions_path: Path = ML3_PREDICTIONS_PATH,
    alert_path: Path = NP3_ALERT_PATH,
) -> tuple[pd.DataFrame, dict]:
    predictions = pd.read_csv(predictions_path)
    predictions = predictions.rename(columns={"target_timestamp": "timestamp"})
    predictions["grid_id"] = predictions["grid_id"].astype(int)
    predictions["timestamp"] = predictions["timestamp"].astype(str)
    alert_keys = _np3_alert_keys(alert_path)
    comparison = predictions[["grid_id", "timestamp", "prediction"]].merge(
        scores[["grid_id", "timestamp", "anomaly_flag", "direction", "anomaly_score"]],
        on=["grid_id", "timestamp"],
        how="inner",
    ).rename(columns={"prediction": "ml3_prediction", "direction": "anomaly_direction"})
    comparison["np3_alert"] = [
        (int(grid_id), timestamp) in alert_keys
        for grid_id, timestamp in zip(comparison["grid_id"], comparison["timestamp"])
    ]
    comparison["all_three_agree"] = (
        comparison["anomaly_flag"] == comparison["ml3_prediction"]
    ) & (comparison["ml3_prediction"] == comparison["np3_alert"])
    comparison = comparison.rename(columns={"timestamp": "target_timestamp"})
    if comparison.empty:
        raise ValueError("No ML3 prediction rows matched anomaly scores.")
    disagreements = comparison[~comparison["all_three_agree"]]
    summary = {
        "compared_rows": len(comparison),
        "all_three_agree": int(comparison["all_three_agree"].sum()),
        "three_way_disagreements": len(disagreements),
        "anomaly_flagged": int(comparison["anomaly_flag"].sum()),
        "ml3_positive": int(comparison["ml3_prediction"].sum()),
        "np3_alerts": int(comparison["np3_alert"].sum()),
        "examples": disagreements.head(10).to_dict(orient="records"),
        "observations": [
            "The anomaly scorer compares each hour with the same grid and hour-of-day historical median, so it can flag both high and low deviations.",
            "ML3 predicts a future proxy label from trailing features, while the anomaly score describes the observed target hour; disagreement is expected because the mechanisms answer different questions.",
            "NP3 uses its operational rule thresholds and contributed only the alerts present in the comparison timestamps; an absent alert is not evidence that the anomaly or classifier is wrong.",
        ],
    }
    return comparison, summary


def build_anomaly_artifacts(
    database_path: Path = WAREHOUSE_PATH,
    output_dir: Path = OUTPUT_DIR,
) -> dict:
    hourly_activity = load_hourly_activity(database_path)
    scores = calculate_anomaly_scores(hourly_activity)
    persisted_rows = persist_anomaly_scores(scores, database_path)
    comparison, summary = compare_three_mechanisms(scores)
    output_dir.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(output_dir / "three_way_comparison.csv", index=False)
    report = {
        "anomaly_threshold_percent": ANOMALY_THRESHOLD_PERCENT,
        "baseline": "median by grid_id and hour_of_day, excluding current row",
        "bucket_counts": {
            "minimum": int(scores["baseline_bucket_count"].min()),
            "maximum": int(scores["baseline_bucket_count"].max()),
            "distinct": int(scores["baseline_bucket_count"].nunique()),
        },
        "score_counts": {
            "rows": len(scores),
            "flagged": int(scores["anomaly_flag"].sum()),
            "high": int(((scores["anomaly_flag"]) & (scores["direction"] == "HIGH")).sum()),
            "low": int(((scores["anomaly_flag"]) & (scores["direction"] == "LOW")).sum()),
        },
        "persisted_rows": persisted_rows,
        "three_way_comparison": summary,
    }
    (output_dir / "anomaly_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    print(json.dumps(build_anomaly_artifacts(), indent=2))
