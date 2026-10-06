from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health():
    response = client.get("/health")

    assert response.status_code == 200

    body = response.json()

    assert body["status"] == "ok"
    assert body["service"] == "network-intelligence-api"


def test_network_summary():
    response = client.get("/network/summary")

    assert response.status_code == 200

    body = response.json()

    assert "total_activity" in body
    assert "active_grids" in body
    assert "peak_hour" in body
    assert "top_grid" in body
    assert "as_of" in body

    assert body["total_activity"] > 0
    assert body["active_grids"] > 0
    assert body["top_grid"] > 0
    assert body["as_of"] == "2013-11-07T19:00:00"


def test_network_summary_explicit_as_of():
    response = client.get(
        "/network/summary",
        params={
            "as_of": "2013-11-07T19:00:00",
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["as_of"] == "2013-11-07T19:00:00"


def test_network_summary_explicit_as_of_is_repeatable():
    params = {
        "as_of": "2013-11-07T19:00:00",
    }

    first = client.get("/network/summary", params=params)
    second = client.get("/network/summary", params=params)

    assert first.status_code == 200
    assert second.status_code == 200

    assert first.json() == second.json()