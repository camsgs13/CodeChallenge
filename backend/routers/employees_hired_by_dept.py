from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sqlite3
import os
import logging
from typing import List, Dict


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router_employees_hired_by_dept = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)

# Define el esquema de salida de la query solicitada
class EmployeeDepQuerySchema(BaseModel):
    id: int
    department: str
    hired: int


# Endpoint para crear la consulta solicitada
@router_employees_hired_by_dept.get("/employees_hired_by_dept", response_model=List[EmployeeDepQuerySchema])
def employees_hired_by_dept(year: str):
    logging.info("Ejecuta: employees_hired_by_dept")
    try:
        results = get_employees_hired_by_dept(year)
        if not results:
            raise HTTPException(status_code=404, detail="No hay informacion para el anio especificado")
        return results
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Error base de datos: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# Ejecuta la query en la base de datos
def get_employees_hired_by_dept(year):
    logging.info("Ejecuta: get_employees_hired_by_dept")
    conn = get_db_connection()
    cursor = conn.cursor()

    query = f"""
    WITH department_hires AS (
        SELECT 
            t2.id,
            t2.department,
            COUNT(*) AS hired
        FROM 
            hired_employees t1 INNER JOIN departments t2
        ON t1.department_id = t2.id
        WHERE 
            t1.datetime LIKE '{year}%'
        GROUP BY 
            t2.id, t2.department
    ),
    avg_hires AS (
        SELECT 
            AVG(hired) AS avg_hired
        FROM 
            department_hires
    )
    SELECT 
        t1.id,
        t1.department,
        t1.hired
    FROM 
        department_hires t1, avg_hires t2
    WHERE 
        t1.hired > t2.avg_hired
    ORDER BY 
        t1.hired DESC;
    """

    logging.info("Query hired employees dept:")
    logging.info(query)

    cursor.execute(query)
    results = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    response = [dict(zip(columns, row)) for row in results]
    logging.info(response)
    logging.info('Finaliza llamado a query')

    return response


# Crea la conexion de la API con la base de datos
def get_db_connection():
    logging.info('Ejecuta: get_db_connection')
    logging.info(f'Current working directory: {os.getcwd()}')
    conn = sqlite3.connect("src/database.db")
    return conn
