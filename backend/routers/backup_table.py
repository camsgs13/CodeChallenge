from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sqlite3
import os
import logging
from fastavro import writer, parse_schema

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router_backup_table = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)


# Endpoint para crear el backup de la tabla solicitada
@router_backup_table.post("/backup_table")
def backup_table(table_name: str):
    logging.info("Ejecuta: backup_table")
    try:
        columns, data = get_table_data(table_name)
        if not columns:
            raise HTTPException(status_code=404, detail=f"La tabla {table_name} no se encuentra en la BD")

        avro_file_path = write_avro_file(table_name, columns, data)
        return {"message": "Backup creado exitosamente", "file": avro_file_path}
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=400, detail=f"Error accediendo a la tabla: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# Obtiene los datos de la tabla solicitada
def get_table_data(table_name):
    logging.info("Ejecuta: get_table_data")
    conn = get_db_connection()
    cursor = conn.cursor()
    backup_query = f"SELECT * FROM `{table_name}`"
    logging.info('Backup query:')
    logging.info(backup_query)
    cursor.execute(backup_query)
    data = cursor.fetchall()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    cursor.close()
    conn.close()
    return columns, data

# Escribe en formato AVRO los datos de las tablas solicitadas
def write_avro_file(table_name, columns, data):
    logging.info("Ejecuta: write_avro_file")
    avro_file_path = f"backup_files/{table_name}_backup.avro"
    logging.info('Ruta de backup:')
    logging.info(avro_file_path)
    schema = {
        "type": "record",
        "name": table_name,
        "fields": [{"name": col, "type": ["null", "string"]} for col in columns]
    }
    parsed_schema = parse_schema(schema)

    records = [{columns[i]: str(value) for i, value in enumerate(row)} for row in data]

    with open(avro_file_path, 'wb') as out:
        writer(out, parsed_schema, records)

    return avro_file_path


# Crea la conexion de la API con la base de datos
def get_db_connection():
    logging.info('Ejecuta: get_db_connection')
    logging.info(f'Current working directory: {os.getcwd()}')
    conn = sqlite3.connect("src/database.db")
    return conn

