from fastapi import APIRouter

from phase4.api5.schemas import RiskPredictionRequest, RiskPredictionResponse
from phase4.api5.service import risk_model_service

router = APIRouter()


@router.post(
    "/network/predict-risk",
    response_model=RiskPredictionResponse,
    summary="Predict network risk",
)
def predict_risk(request: RiskPredictionRequest) -> RiskPredictionResponse:
    return risk_model_service.predict(request)
