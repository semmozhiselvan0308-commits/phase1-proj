from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from phase4.api1.app.core.config import WAREHOUSE_PATH
from phase4.api1.app.models.network import NetworkSummaryResponse
from phase4.api1.app.services.network_service import NetworkService


router = APIRouter(
    prefix="/network",
    tags=["Network Intelligence"],
)

service = NetworkService(WAREHOUSE_PATH)


@router.get(
    "/summary",
    response_model=NetworkSummaryResponse,
)
def get_network_summary(
    as_of: datetime | None = Query(
        default=None,
        description="Optional reporting timestamp",
    ),
):
    try:
        return service.get_summary(as_of)

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Analytics data source unavailable: {exc}",
        ) from exc