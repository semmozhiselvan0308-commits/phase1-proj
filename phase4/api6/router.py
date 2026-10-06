from fastapi import APIRouter, HTTPException, status

from phase4.api6.schemas import GridLocationResponse, PipelineStatusResponse
from phase4.api6.service import (
    STATUS_FILE,
    build_pipeline_status,
    get_grid_location,
    read_pipeline_status,
)

router = APIRouter()


@router.get("/pipeline/status", response_model=PipelineStatusResponse)
def pipeline_status() -> PipelineStatusResponse:
    if not STATUS_FILE.exists():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DE7 pipeline status record is unavailable.",
        )
    try:
        return build_pipeline_status(read_pipeline_status(STATUS_FILE))
    except (OSError, ValueError, KeyError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"DE7 pipeline status record is invalid: {error}",
        ) from error


@router.get(
    "/network/grid/{grid_id}/location",
    response_model=GridLocationResponse,
)
def grid_location(grid_id: int) -> GridLocationResponse:
    if grid_id <= 0:
        raise HTTPException(status_code=422, detail="grid_id must be greater than 0.")
    try:
        location = get_grid_location(grid_id)
    except FileNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Grid reference data is unavailable: {error}",
        ) from error
    except (OSError, ValueError, KeyError, LookupError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Grid reference data is invalid: {error}",
        ) from error

    if location is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No grid location found for grid '{grid_id}'.",
        )
    return GridLocationResponse(**location)
