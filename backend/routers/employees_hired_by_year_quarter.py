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

router_employees_year_quarter = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)

# Define el esquema de salida de la query solicitada
class EmployeeQuerySchema(BaseModel):
    department: str
    job: str
    Q1: int
    Q2: int
    Q3: int
    Q4: int
    employee_count: int


# Endpoint para crear la consulta solicitada
@router_employees_year_quarter.get("/employees_hired_by_quarter", response_model=List[EmployeeQuerySchema])
def employees_hired_by_quarter(year: str):
    logging.info("Ejecuta: employees_hired_by_quarter")
    try:
        results = get_employees_hired_by_quarter(year)
        if not results:
            raise HTTPException(status_code=404, detail="No hay informacion para el anio especificado")
        return results
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Error base de datos: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# Ejecuta la query en la base de datos
def get_employees_hired_by_quarter(year):
    logging.info("Ejecuta: get_employees_hired_by_quarter")
    conn = get_db_connection()
    cursor = conn.cursor()

    query = f"""
    SELECT 
        t2.department,
        t3.job,
        SUM(CASE 
            WHEN substring(t1.datetime,1,7) IN ('{year}-01','{year}-02','{year}-03') THEN 1
            ELSE 0
        END) AS Q1,
        SUM(CASE  
            WHEN substring(t1.datetime,1,7) IN ('{year}-04','{year}-05','{year}-06') THEN 1
            ELSE 0
        END) AS Q2,
        SUM(CASE  
            WHEN substring(t1.datetime,1,7) IN ('{year}-07','{year}-08','{year}-09') THEN 1
            ELSE 0
        END) AS Q3,
        SUM(CASE  
            WHEN substring(t1.datetime,1,7) IN ('{year}-10','{year}-11','{year}-12') THEN 1
            ELSE 0
        END) AS Q4,
        COUNT(*) AS employee_count
    FROM hired_employees t1 INNER JOIN departments t2
    ON t1.job_id = t2.id
    INNER JOIN jobs t3
    ON t1.job_id = t3.id
    WHERE 
        t1.datetime LIKE '{year}%'
    GROUP BY 
        department, job
    ORDER BY 
        department, job;
    """

    logging.info("Query hired employees quarter:")
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
