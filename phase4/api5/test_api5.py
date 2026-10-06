import pytest
from fastapi.testclient import TestClient

from phase4.api5.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def valid_payload() -> dict:
    return {
        "grid_id": 4821,
        "feature_timestamp": "2013-11-07T19:00:00Z",
        "avg_activity": 42.5,
        "activity_growth": 0.12,
        "active_hours": 18.0,
        "peak_ratio": 1.4,
        "variability": 0.25,
        "internet_share": 0.85,
    }


def test_predict_risk_uses_trained_model_and_preserves_contract(client: TestClient):
    response = client.post("/network/predict-risk", json=valid_payload())

    assert response.status_code == 200

    body = response.json()

    # ML5 must preserve the existing response shape.
    assert set(body) == {
        "risk_score",
        "risk_level",
        "model_version",
        "explanation_note",
    }

    # The trained model must return a normalized probability.
    assert 0.0 <= body["risk_score"] <= 1.0

    # Risk level is derived from the model probability.
    expected_level = (
        "HIGH"
        if body["risk_score"] >= 0.75
        else "MEDIUM"
        if body["risk_score"] >= 0.5
        else "LOW"
    )
    assert body["risk_level"] == expected_level

    # ML5 must report the real trained model rather than the old stub.
    assert body["model_version"].startswith("ml3-")
    assert "ML3 trained classifier" in body["explanation_note"]


def test_predict_risk_rejects_missing_required_field(client: TestClient):
    payload = valid_payload()
    del payload["internet_share"]

    response = client.post("/network/predict-risk", json=payload)

    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["loc"] == ["body", "internet_share"]
    assert "Field required" in error["msg"]


def test_predict_risk_rejects_out_of_range_feature(client: TestClient):
    payload = valid_payload()
    payload["internet_share"] = 1.5

    response = client.post("/network/predict-risk", json=payload)

    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["loc"] == ["body", "internet_share"]
    assert "less than or equal to 1" in error["msg"]
