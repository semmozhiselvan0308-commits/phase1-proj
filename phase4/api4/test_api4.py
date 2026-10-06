import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

# 1. Import your FastAPI router
from phase4.api4.router import router
from phase4.api4.schemas import GridFeaturesResponse

# 2. Instantiate a temporary app for testing (or import your main app)
app = FastAPI()
app.include_router(router)


# 3. Define the missing 'client' fixture
@pytest.fixture
def client():
    return TestClient(app)


# --- Tests ---


def test_get_grid_features_success(client: TestClient):
    # Adjust 'grid_123' to a grid ID that exists in your stored data source
    response = client.get("/network/grid/grid_123/features")
    assert response.status_code == 200

    data = response.json()
    expected_keys = {
        "grid_id",
        "feature_timestamp",
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share",
        "freshness_seconds",
        "data_quality_status",
    }
    assert expected_keys.issubset(data.keys())


def test_get_grid_features_not_found(client: TestClient):
    response = client.get("/network/grid/non_existent_grid/features")
    assert response.status_code == 404