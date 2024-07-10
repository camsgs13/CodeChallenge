from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sqlite3
import os
import logging
from fastavro import reader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router_restore_table = APIRouter(
    prefix="/code-challenge",
    tags=["items"],
)


# Endpoint para crear el restore de la tabla solicitada
@router_restore_table.post("/restore_table")
def restore_table(table_name: str):
    logging.info("Ejecuta: restore_table")
    try:
        restore_table_from_avro(table_name)
        return {"message": f"La tabla {table_name} se ha restablecido correctamente desde su backup"}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=400, detail=f"Error accessing table: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")



# Lee los datos del backup
def read_avro_file(table_name):
    logging.info("Ejecuta: read_avro_file")
    avro_file_path = f"backup_files/{table_name}_backup.avro"
    logging.info("avro_file_path")
    logging.info(avro_file_path)
    with open(avro_file_path, 'rb') as fo:
        avro_reader = reader(fo)
        schema = avro_reader.writer_schema
        rows = [row for row in avro_reader]

    return schema, rows


# Envia a la base de datos el backup obtenido
def restore_table_from_avro(table_name):
    schema, rows = read_avro_file(table_name)
    columns = [field["name"] for field in schema["fields"]]
    conn = get_db_connection()
    cursor = conn.cursor()

    # Elimina el contenido de la tabla
    cursor.execute(f"DELETE FROM {table_name}")

    # Ingresa en la tabla de la base de datos el contenido del backup
    for row in rows:
        placeholders = ", ".join(["?" for col in columns])
        column_names = ", ".join(columns)
        values = [row[col] for col in columns]
        query_insert = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        cursor.execute(query_insert, values)

    conn.commit()
    cursor.close()
    conn.close()


# Crea la conexion de la API con la base de datos
def get_db_connection():
    logging.info('Ejecuta: get_db_connection')
    logging.info(f'Current working directory: {os.getcwd()}')
    conn = sqlite3.connect("src/database.db")
    return conn

