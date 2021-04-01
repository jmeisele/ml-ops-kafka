"""
Author: Jason Eisele
Date: December 2, 2020
Scope: Prediction route responsding to POST requests w/ background task
"""
# Incoming payload data model
from app.data_models.payload import HousePredictionPayload
# Outbound prediction result data model
from app.data_models.prediction import HousePredictionResult
# ML Model object itself
from app.services.models import HousePriceModel
# Background task
from app.api.tasks.send_kafka import add_message_to_kafka

from starlette.requests import Request
from fastapi import APIRouter, BackgroundTasks

router = APIRouter()


@router.post("/predict", response_model=HousePredictionResult, name="predict")
async def post_predict(request: Request,
                       backgound_tasks: BackgroundTasks,
                       block_data: HousePredictionPayload = None,
                       ) -> HousePredictionResult:
    model: HousePriceModel = request.app.state.model
    prediction: HousePredictionResult = model.predict(block_data)
    backgound_tasks.add_task(
        add_message_to_kafka,
        topic="ml-ops",
        value={
            "request": await request.json(),
            "median_house_value": prediction.median_house_value,
            "model_version": model.version
            }
        )
    return prediction
