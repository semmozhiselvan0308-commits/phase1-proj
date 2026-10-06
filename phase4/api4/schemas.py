from datetime import datetime
from enum import Enum
from pydantic import BaseModel, ConfigDict


class QualityStatus(str, Enum):
    GOOD = "GOOD"
    STALE = "STALE"


class GridFeaturesResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    grid_id: str
    feature_timestamp: datetime
    # ML2 Features
    avg_activity: float
    activity_growth: float
    active_hours: float
    peak_ratio: float
    variability: float
    internet_share: float
    # Freshness
    freshness_seconds: float
    data_quality_status: QualityStatus