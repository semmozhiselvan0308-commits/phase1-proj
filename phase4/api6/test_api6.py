import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from phase4.api6 import router as router_module
from phase4.api6.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def write_status(path, pipeline_result="success"):
    path.write_text(
        json.dumps(
            {
                "run_id": "de7_test_run",
                "run_timestamp": "2026-09-06T10:00:00+00:00",
                "tasks": {
                    "ingest": "success",
                    "validate": "success",
                    "spark_process": "success",
                    "load_warehouse": "success",
                    "quality_check": "success",
                    "notify": "success",
                },
                "metrics": {
                    "rows_in": 100,
                    "rows_rejected": 2,
                    "nulls_handled": 3,
                    "rows_published": 95,
                },
                "AS_OF": "2013-11-07T19:00:00",
                "pipeline_result": pipeline_result,
            }
        ),
        encoding="utf-8",
    )


def test_pipeline_status_exposes_de7_evidence(client, tmp_path, monkeypatch):
    status_path = tmp_path / "pipeline_status.json"
    write_status(status_path)
    monkeypatch.setattr(router_module, "STATUS_FILE", status_path)

    response = client.get("/pipeline/status")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "de7_test_run"
    assert body["rows_in"] == 100
    assert body["rows_rejected"] == 2
    assert body["nulls_handled"] == 3
    assert body["rows_published"] == 95
    assert body["AS_OF"] == "2013-11-07T19:00:00"
    assert body["healthy"] is True
    assert body["reasons"] == []


def test_pipeline_status_reports_unhealthy_after_failed_run(
    client, tmp_path, monkeypatch
):
    status_path = tmp_path / "pipeline_status.json"
    write_status(status_path, pipeline_result="failure")
    failed_status = json.loads(status_path.read_text(encoding="utf-8"))
    failed_status["tasks"]["spark_process"] = "failed"
    status_path.write_text(json.dumps(failed_status), encoding="utf-8")
    monkeypatch.setattr(router_module, "STATUS_FILE", status_path)

    response = client.get("/pipeline/status")

    assert response.status_code == 200
    body = response.json()
    assert body["healthy"] is False
    assert "DE7 pipeline_result is not success." in body["reasons"]
    assert "Task 'spark_process' has status 'failed'." in body["reasons"]


def test_grid_4821_location_has_centroid_without_geometry(client):
    response = client.get("/network/grid/4821/location")

    assert response.status_code == 200
    body = response.json()
    assert body["grid_id"] == 4821
    assert 45.3 < body["centroid_latitude"] < 45.6
    assert 8.8 < body["centroid_longitude"] < 9.3
    assert "polygon_reference" in body
    assert "geometry" not in body
    assert "coordinates" not in body


def test_pipeline_status_missing_record_returns_503(client, tmp_path, monkeypatch):
    monkeypatch.setattr(router_module, "STATUS_FILE", tmp_path / "missing.json")

    response = client.get("/pipeline/status")

    assert response.status_code == 503
