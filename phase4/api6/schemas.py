from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class PipelineStatusResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    run_timestamp: datetime
    tasks: dict[str, str]
    rows_in: int = Field(ge=0)
    rows_rejected: int = Field(ge=0)
    nulls_handled: int = Field(ge=0)
    rows_published: int = Field(ge=0)
    AS_OF: datetime | None
    analytics_age_seconds: float | None = Field(default=None, ge=0)
    analytics_freshness: str
    healthy: bool
    reasons: list[str]


class GridLocationResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    grid_id: int
    centroid_latitude: float
    centroid_longitude: float
    polygon_reference: str
