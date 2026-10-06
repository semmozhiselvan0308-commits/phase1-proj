from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, status
import pandas as pd
from phase4.api4.schemas import GridFeaturesResponse, QualityStatus

router = APIRouter()


def get_stored_features(grid_id: str) -> dict | None:
    # Adjust path to match your ML2 output table location
    feature_store_path = "data/landing/features.parquet"

    try:
        df = pd.read_parquet(feature_store_path)
        row = df[df["grid_id"] == grid_id]
        if row.empty:
            return None
        return row.iloc[0].to_dict()
    except Exception:
        # Fallback dictionary for testing if data store isn't populated yet
        mock_data = {
            "grid_123": {
                "feature_timestamp": datetime.now(timezone.utc),
                "avg_activity": 42.5,
                "activity_growth": 0.12,
                "active_hours": 18.0,
                "peak_ratio": 1.4,
                "variability": 0.25,
                "internet_share": 0.85,
            }
        }
        return mock_data.get(grid_id)


@router.get(
    "/network/grid/{grid_id}/features", response_model=GridFeaturesResponse
)
def get_grid_features(grid_id: str):
    record = get_stored_features(grid_id)

    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No stored ML features found for grid '{grid_id}'.",
        )

    now = datetime.now(timezone.utc)
    timestamp = record["feature_timestamp"]
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    freshness = (now - timestamp).total_seconds()
    quality = QualityStatus.GOOD if freshness < 3600 else QualityStatus.STALE

    return GridFeaturesResponse(
        grid_id=grid_id,
        feature_timestamp=timestamp,
        avg_activity=record["avg_activity"],
        activity_growth=record["activity_growth"],
        active_hours=record["active_hours"],
        peak_ratio=record["peak_ratio"],
        variability=record["variability"],
        internet_share=record["internet_share"],
        freshness_seconds=freshness,
        data_quality_status=quality,
    )