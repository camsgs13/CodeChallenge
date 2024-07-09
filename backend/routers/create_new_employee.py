from fastapi import APIRouter, HTTPException
from routers.health import router
from pydantic import BaseModel
import sqlite3
import os
import logging

router_create_new_employee = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)

logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S'
  )

# Crea la conexion de la API con la base de datos
def get_db_connection():
    logging.info(os.getcwd())
    conn = sqlite3.connect("config/database.db")
    return conn

# Se crea el modelo de los datos esperados
class HiredEmployeesIn(BaseModel):
    id:int
    name:str
    datetime:str
    department_id:int
    job_id:int

# Endpoint para manejar la llegada de un nuevo dato a hired_employees
@router_create_new_employee.post('/create-new-employee', tags=['Add'])
def create_new_employee(employee: HiredEmployeesIn):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Hace un INSERT de los valores obtenido por la API
        cursor.execute("INSERT INTO transactions (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)",
                       (employee.id, employee.name, employee.datetime, employee.department_id, employee.job_id))
        conn.commit()

        # Devuelve el ID del registro recien creado para confirmar que fue ingresado
        employee_id = cursor.lastrowid

        # Cierra la conexion con la base de datos
        cursor.close()
        conn.close()
        return {"message": "Se ha ingresado correctamente el registro", "employee_id": employee_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se ha podido ingresar el registro: {str(e)}")
    



router_test = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)
@router_test.get('/test_cwd', tags=['Test'])
def test_cwd():
    cwd = os.getcwd()
    response = os.getcwd()
    table_response = 'No se pudo validar en la BD'
    try:
        logging.info('Intenta conexion con BD')
        conn = get_db_connection()
        cursor = conn.cursor()
        response = 'Se conecto a la base de datos'
    except:
        logging.info('No se pudo conectar con BD')
        response = 'No se pudo conectar'
    try:
        # conn = get_db_connection()
        # conn = sqlite3.connect("config/database.db")
        logging.info('Intenta consultar las tablas de la BD')
        # cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';") 
        tables = cursor.fetchall()
        tables = [table[0] for table in tables]
        # table_response = ','.join(tables)
        table_response = tables
        table_response = conn
    except:
        logging.info('No pudo consultar las tablas de la BD')
        table_response = f'No se pudo validar las tablas en la BD'
    return {'cwd':cwd,
            'response': response,
            'table':table_response}