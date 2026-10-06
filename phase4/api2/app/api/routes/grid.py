from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from phase4.api2.app.core.config import WAREHOUSE_PATH
from phase4.api2.app.models.grid import GridActivityPoint
from phase4.api2.app.services.grid_service import GridService


router = APIRouter(
    prefix="/network",
    tags=["Network Intelligence"],
)

service = GridService(WAREHOUSE_PATH)


@router.get(
    "/grid/{grid_id}",
    response_model=list[GridActivityPoint],
)
def get_grid_activity(
    grid_id: int,
    date: str | None = Query(
        default=None,
        description="Optional date filter in YYYY-MM-DD format",
    ),
    hour: int | None = Query(
        default=None,
        ge=0,
        le=23,
        description="Optional hour filter from 0 to 23",
    ),
    as_of: datetime | None = Query(
        default=None,
        description="Optional reporting timestamp",
    ),
):
    try:
        if not service.grid_exists(grid_id):
            raise HTTPException(
                status_code=404,
                detail=f"Grid {grid_id} not found",
            )

        return service.get_activity(
            grid_id=grid_id,
            date=date,
            hour=hour,
            as_of=as_of,
        )

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Analytics data source unavailable: {exc}",
        ) from exc