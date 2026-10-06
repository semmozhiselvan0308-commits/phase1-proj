from datetime import datetime

from pydantic import BaseModel


class GridActivityPoint(BaseModel):
    timestamp: datetime
    date: str
    hour: int
    sms_activity: float
    call_activity: float
    internet_activity: float
    total_activity: float