from fastapi import APIRouter
import logging
from typing import Optional
from typing import Sequence

router = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)

@router.get('/health', status_code=200, tags=['Health'])
def health_msg():
    logging.info("/health inicia ejecución exitosa")
    logging.info("Retornando respuesta")
    return {'status': 'Service UP'}
