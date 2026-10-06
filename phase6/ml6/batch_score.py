import hashlib
import json
import pickle
import sqlite3
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

WAREHOUSE_PATH = (
    PROJECT_ROOT
    / "phase3"
    / "de6"
    / "warehouse"
    / "network_analytics.db"
)

MODEL_PATH = (
    PROJECT_ROOT
    / "phase6"
    / "ml3"
    / "outputs"
    / "risk_classifier.pkl"
)

REPORT_PATH = (
    PROJECT_ROOT
    / "phase6"
    / "ml3"
    / "outputs"
    / "evaluation_report.json"
)

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
BATCH_REPORT_PATH = OUTPUT_DIR / "batch_score_report.json"

FEATURE_NAMES = (
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
)

RISK_SCORE_COLUMNS = (
    "grid_id",
    "feature_timestamp",
    "risk_score",
    "risk_level",
    "model_version",
)


def load_model(
    model_path: Path = MODEL_PATH,
    report_path: Path = REPORT_PATH,
):
    if not model_path.exists():
        raise FileNotFoundError(
            f"Trained risk model artifact is missing: {model_path}"
        )

    with model_path.open("rb") as file:
        model = pickle.load(file)

    if not hasattr(model, "predict_proba"):
        raise TypeError(
            "The trained risk model must expose predict_proba()."
        )

    digest = hashlib.sha256(model_path.read_bytes()).hexdigest()[:12]

    algorithm = "trained-model"

    if report_path.exists():
        report = json.loads(
            report_path.read_text(encoding="utf-8")
        )
        algorithm = str(
            report.get("algorithm", algorithm)
        )

    normalized = "-".join(
        algorithm.lower()
        .replace("+", " ")
        .split()
    )

    model_version = f"ml3-{normalized}-{digest}"

    return model, model_version


def load_feature_table(
    database_path: Path = WAREHOUSE_PATH,
) -> pd.DataFrame:
    if not database_path.exists():
        raise FileNotFoundError(
            f"Warehouse database is missing: {database_path}"
        )

    columns = ", ".join(
        ["grid_id", "feature_timestamp", *FEATURE_NAMES]
    )

    with sqlite3.connect(database_path) as connection:
        dataframe = pd.read_sql_query(
            f"""
            SELECT {columns}
            FROM network_feature_table
            ORDER BY feature_timestamp, grid_id
            """,
            connection,
        )

    if dataframe.empty:
        raise ValueError(
            "network_feature_table is empty."
        )

    expected_columns = {
        "grid_id",
        "feature_timestamp",
        *FEATURE_NAMES,
    }

    if set(dataframe.columns) != expected_columns:
        raise ValueError(
            "network_feature_table schema does not match ML2."
        )

    return dataframe


def calculate_risk_scores(
    features: pd.DataFrame,
    model,
    model_version: str,
) -> pd.DataFrame:
    missing = [
        name
        for name in FEATURE_NAMES
        if name not in features.columns
    ]

    if missing:
        raise ValueError(
            f"Missing ML2 feature columns: {missing}"
        )

    model_input = features.loc[:, FEATURE_NAMES].to_numpy(dtype=float)

    probabilities = model.predict_proba(
        model_input
    )

    classes = list(
        getattr(model, "classes_", [0, 1])
    )

    if 1 not in classes:
        raise ValueError(
            "The trained model does not contain positive class 1."
        )

    positive_index = classes.index(1)

    risk_scores = probabilities[:, positive_index]

    result = features[
        ["grid_id", "feature_timestamp"]
    ].copy()

    result["risk_score"] = pd.Series(
        risk_scores,
        index=result.index,
    ).clip(0.0, 1.0)

    result["risk_level"] = pd.cut(
        result["risk_score"],
        bins=[-float("inf"), 0.5, 0.75, float("inf")],
        labels=["LOW", "MEDIUM", "HIGH"],
        right=False,
    ).astype(str)

    result["risk_score"] = result[
        "risk_score"
    ].round(6)

    result["model_version"] = model_version

    return result[list(RISK_SCORE_COLUMNS)]


def persist_risk_scores(
    scores: pd.DataFrame,
    database_path: Path = WAREHOUSE_PATH,
) -> int:
    if scores.empty:
        return 0

    rows = [
        (
            int(row.grid_id),
            str(row.feature_timestamp),
            float(row.risk_score),
            str(row.risk_level),
            str(row.model_version),
        )
        for row in scores.itertuples(index=False)
    ]

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS network_risk_scores (
                grid_id INTEGER NOT NULL,
                feature_timestamp TEXT NOT NULL,
                risk_score REAL NOT NULL,
                risk_level TEXT NOT NULL,
                model_version TEXT NOT NULL,
                PRIMARY KEY (grid_id, feature_timestamp)
            )
            """
        )

        connection.executemany(
            """
            INSERT INTO network_risk_scores (
                grid_id,
                feature_timestamp,
                risk_score,
                risk_level,
                model_version
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(grid_id, feature_timestamp)
            DO UPDATE SET
                risk_score = excluded.risk_score,
                risk_level = excluded.risk_level,
                model_version = excluded.model_version
            """,
            rows,
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_network_risk_scores_timestamp
            ON network_risk_scores (feature_timestamp)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_network_risk_scores_level
            ON network_risk_scores (risk_level)
            """
        )

        connection.commit()

    return len(rows)


def build_batch_report(
    features: pd.DataFrame,
    scores: pd.DataFrame,
    model_version: str,
) -> dict:
    report = {
        "status": "SUCCESS",
        "model_version": model_version,
        "input_rows": int(len(features)),
        "scored_rows": int(len(scores)),
        "distinct_grids": int(scores["grid_id"].nunique()),
        "distinct_feature_timestamps": int(
            scores["feature_timestamp"].nunique()
        ),
        "risk_level_counts": {
            str(level): int(
                (scores["risk_level"] == level).sum()
            )
            for level in ["LOW", "MEDIUM", "HIGH"]
        },
        "risk_score_range": {
            "minimum": float(scores["risk_score"].min()),
            "maximum": float(scores["risk_score"].max()),
        },
        "feature_timestamp_min": str(
            scores["feature_timestamp"].min()
        ),
        "feature_timestamp_max": str(
            scores["feature_timestamp"].max()
        ),
        "warehouse_table": "network_risk_scores",
        "primary_key": [
            "grid_id",
            "feature_timestamp",
        ],
        "rerun_safe": True,
    }

    return report


def run_batch_scoring(
    database_path: Path = WAREHOUSE_PATH,
) -> dict:
    model, model_version = load_model()

    features = load_feature_table(database_path)

    scores = calculate_risk_scores(
        features,
        model,
        model_version,
    )

    persisted_rows = persist_risk_scores(
        scores,
        database_path,
    )

    report = build_batch_report(
        features,
        scores,
        model_version,
    )

    report["persisted_rows"] = persisted_rows

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    BATCH_REPORT_PATH.write_text(
        json.dumps(
            report,
            indent=2,
        ),
        encoding="utf-8",
    )

    return report


if __name__ == "__main__":
    print(
        json.dumps(
            run_batch_scoring(),
            indent=2,
        )
    )
