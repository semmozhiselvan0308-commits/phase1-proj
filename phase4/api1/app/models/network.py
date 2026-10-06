from datetime import datetime

from pydantic import BaseModel


class NetworkSummaryResponse(BaseModel):
    total_activity: float
    active_grids: int
    peak_hour: datetime
    top_grid: int
    as_of: datetime