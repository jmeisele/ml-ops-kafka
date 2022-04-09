"""
Author: Jason Eisele
Date: December 2, 2020
Scope: API app handler, supports events that happen on startup/shutdown
"""

from typing import Callable

from confluent_kafka import Producer
from fastapi import FastAPI
from loguru import logger

from app.core.config import DEFAULT_MODEL_PATH, KAFKA_CONFIG
from app.services.models import HousePriceModel


def _startup_model(app: FastAPI) -> None:
    model_instance = HousePriceModel(DEFAULT_MODEL_PATH)
    app.state.model = model_instance

def _startup_kafka_client(app: FastAPI) -> None:
    app.state.kafka_producer = Producer(**KAFKA_CONFIG)

def _shutdown_model(app: FastAPI) -> None:
    app.state.model = None

def _shutdown_kafka(app: FastAPI) -> None:
    app.state.kafka_producer.flush()

def start_app_handler(app: FastAPI) -> Callable:
    def startup() -> None:
        logger.info("Running app start handler.")
        _startup_model(app)
        _startup_kafka_client(app)
    return startup


def stop_app_handler(app: FastAPI) -> Callable:
    def shutdown() -> None:
        logger.info("Running app shutdown handler.")
        _shutdown_model(app)
        _shutdown_kafka(app)
    return shutdown
