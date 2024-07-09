from fastapi import APIRouter
import logging

router = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)

@router.get('/health', status_code=200, tags=['Health'])
def health_msg():
    logging.info("/health inicia ejecución exitosa")
    logging.info("Retornando respuesta")
    return {'status': 'Service UP'}
