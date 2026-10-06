from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class RiskPredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grid_id: int = Field(gt=0, description="Positive Milan grid identifier.")
    feature_timestamp: datetime
    avg_activity: float = Field(ge=0)
    activity_growth: float
    active_hours: float = Field(ge=0, le=24)
    peak_ratio: float = Field(ge=0)
    variability: float = Field(ge=0)
    internet_share: float = Field(ge=0, le=1)


class RiskPredictionResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    risk_score: float = Field(ge=0, le=1)
    risk_level: str
    model_version: str
    explanation_note: str
