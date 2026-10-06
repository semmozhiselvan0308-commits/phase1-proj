import hashlib
import json
import pickle
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from phase4.api5.schemas import RiskPredictionRequest, RiskPredictionResponse


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = PROJECT_ROOT / "phase6" / "ml3" / "outputs" / "risk_classifier.pkl"
REPORT_PATH = PROJECT_ROOT / "phase6" / "ml3" / "outputs" / "evaluation_report.json"
WAREHOUSE_PATH = PROJECT_ROOT / "phase3" / "de6" / "warehouse" / "network_analytics.db"
FEATURE_NAMES = (
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
)


class RiskModelService:
    def __init__(
        self,
        model_path: Path = MODEL_PATH,
        warehouse_path: Path = WAREHOUSE_PATH,
        report_path: Path = REPORT_PATH,
    ) -> None:
        if not model_path.exists():
            raise FileNotFoundError(
                f"Trained risk model artifact is missing: {model_path}"
            )
        try:
            with model_path.open("rb") as file:
                self.model = pickle.load(file)
        except (OSError, pickle.PickleError, ModuleNotFoundError, ImportError) as error:
            raise RuntimeError(
                f"The trained risk model could not be loaded from {model_path}: {error}"
            ) from error
        if not hasattr(self.model, "predict_proba"):
            raise TypeError("The trained risk model must expose predict_proba().")

        self.warehouse_path = warehouse_path
        self.model_version = self._model_version(model_path, report_path)

    @staticmethod
    def _model_version(model_path: Path, report_path: Path) -> str:
        digest = hashlib.sha256(model_path.read_bytes()).hexdigest()[:12]
        algorithm = "trained-model"
        if report_path.exists():
            report = json.loads(report_path.read_text(encoding="utf-8"))
            algorithm = str(report.get("algorithm", algorithm))
        normalized = "-".join(algorithm.lower().replace("+", " ").split())
        return f"ml3-{normalized}-{digest}"

    def predict(self, request: RiskPredictionRequest) -> RiskPredictionResponse:
        values = [[getattr(request, name) for name in FEATURE_NAMES]]
        probabilities = self.model.predict_proba(values)[0]
        classes = list(getattr(self.model, "classes_", [0, 1]))
        positive_index = classes.index(1) if 1 in classes else len(probabilities) - 1
        risk_score = float(probabilities[positive_index])
        risk_level = (
            "HIGH" if risk_score >= 0.75
            else "MEDIUM" if risk_score >= 0.5
            else "LOW"
        )
        anomaly = self._find_anomaly(request)
        anomaly_note = self._anomaly_note(anomaly)
        return RiskPredictionResponse(
            risk_score=round(max(0.0, min(1.0, risk_score)), 6),
            risk_level=risk_level,
            model_version=self.model_version,
            explanation_note=(
                f"ML3 trained classifier scored the supplied feature window. "
                f"{anomaly_note}"
            ),
        )

    def _find_anomaly(self, request: RiskPredictionRequest) -> dict[str, Any] | None:
        if not self.warehouse_path.exists():
            return None
        timestamp = request.feature_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        try:
            with sqlite3.connect(self.warehouse_path) as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    """
                    SELECT timestamp, anomaly_score, anomaly_flag, direction, reason
                    FROM network_anomaly_scores
                    WHERE grid_id = ? AND timestamp <= ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (request.grid_id, timestamp),
                ).fetchone()
        except sqlite3.Error:
            return None
        return dict(row) if row is not None else None

    @staticmethod
    def _anomaly_note(anomaly: dict[str, Any] | None) -> str:
        if anomaly is None:
            return "No persisted ML4 anomaly score was found for this grid and feature timestamp."
        flag = "flagged" if anomaly["anomaly_flag"] else "not flagged"
        return (
            f"ML4 anomaly score {float(anomaly['anomaly_score']):.2f}% ({flag}, "
            f"{anomaly['direction']}) from observation {anomaly['timestamp']}."
        )


risk_model_service = RiskModelService()
