from fastapi import APIRouter, HTTPException
# from routers.health import router
from pydantic import BaseModel
import sqlite3
import os
import logging

router_test = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)


@router_test.get('/test_cwd', tags=['Test'])
def test_cwd():
    logging.info('Ejecuta: test_cwd')
    cwd = os.getcwd()
    logging.info(f'Current directory: {cwd}')

    # Se conecta a la BD y realiza prueba de listado de tablas
    try:
        conn = get_db_connection()
        logging.info('Se conecto la BD')
        response = 'Se conecto a la BD'
        cursor = conn.cursor()
        test_result = list_tables_new_db(cursor)
        logging.info('Resultado de pruebas sobre la BD')
        logging.info(test_result)
        cursor.close()
        conn.close()
    except:
        response = 'No se pudo conectar a la BD'
        test_result = 'No se puede realizar la prueba de conexion'

    return {'cwd':cwd,
            'response':response,
            'test_result':test_result}


def get_db_connection():
    logging.info('Ejecuta: get_db_connection')
    logging.info(f'Current working directory: {os.getcwd()}')
    conn = sqlite3.connect("src/database.db")
    return conn

def list_tables_new_db(cursor):
    logging.info('Ejecuta: list_tables_new_db')
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    tables = [table[0] for table in tables]
    tables_list = ','.join(tables)
    logging.info('Listado de tablas:')
    logging.info(tables_list)
    return tables_list