from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sqlite3
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router_create_new_department = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)


# Se crea el modelo de los datos esperados
class Department(BaseModel):
    id: int
    department: str


# Endpoint para manejar la llegada de un nuevo dato a hired_employees
@router_create_new_department.post('/create-new-department', tags=['Add Data'])
def create_new_department(dept: Department):
    logging.info('Ejecuta: create_new_department')
    # Lee los datos del request
    id = dept.id
    department = dept.department
    logging.info('Datos de request obtenidos')
    # return de prueba de los datos
    # return {'id':id, 'department':department}

    # Se conecta a la BD para hacer el insert
    try:
        logging.info('Inicia conexion con la BD')
        conn = get_db_connection()
        cursor = conn.cursor()
        # Hace un INSERT de los datos obtenidos por la API
        logging.info('Inicia INSERT a la base de datos')
        cursor.execute("INSERT INTO departments (id, department) VALUES (?, ?)",
                       (id, department))
        # Devuelve el ID del registro recien creado para confirmar que fue ingresado
        dept_id = cursor.lastrowid
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Se ha ingresado correctamente el registro", "dept_row_id": dept_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se ha podido ingresar el registro: {str(e)}")


# Crea la conexion de la API con la base de datos
def get_db_connection():
    logging.info('Ejecuta: get_db_connection')
    logging.info(f'Current working directory: {os.getcwd()}')
    conn = sqlite3.connect("src/database.db")
    return conn
