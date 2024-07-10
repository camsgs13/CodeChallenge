"""
Este script es el encargado de migrar los archivos a la nueva base de datos

Este script realiza las siguientes tareas:
1. Crea la conexion a la base de datos SQLite3
2. Encuentra en la carpeta origen los archivos que deben ser migrados
3. Envía cada uno de los archivos a la base de datos

Para efectos del ejercicio, por defecto se leeran archivos XLSX y la validacion de esquema
"""
import pandas as pd
import sqlite3
import os
import logging

logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S'
)


def create_data_base():
    logging.info('Ejecuta: create_data_base')
    os.system('cls')
    logging.info(os.getcwd())
    os.chdir('..')
    os.chdir('src')
    logging.info(os.getcwd())
    # Crea la conexion con la base de datos a usar
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.close()
    conn.close()


def get_db_connection():
    logging.info('Ejecuta: get_db_connection')
    logging.info(os.getcwd())
    conn = sqlite3.connect("database.db")
    return conn


def move_files_to_db():
    logging.info('Ejecuta: get_db_connection')
    conn = get_db_connection()

    # Inicia la lectura de los archivos
    # Archivo hired_employees.xlsx
    columns = ['id','name','datetime','department_id','job_id']
    data_types = {'id':'Int64',
                'name':str,
                'datetime':str,
                'department_id':'Int64',
                'job_id':'Int64'}
    df_hired_employees = pd.read_excel('hired_employees.xlsx',names=columns, dtype=data_types,header=None)
    # print(df_hired_employees.info())
    # print(df_hired_employees.head())

    # Archivo departments.xlsx
    columns = ['id','department']
    data_types = {'id':'Int64',
                'department':str}
    df_departments = pd.read_excel('departments.xlsx',names=columns, dtype=data_types,header=None)
    # print(df_departments.info())
    # print(df_departments.head())

    # Archivo jobs.xlsx
    columns = ['id','job']
    data_types = {'id':'Int64',
                'job':str}
    df_jobs = pd.read_excel('jobs.xlsx',names=columns, dtype=data_types,header=None)
    # print(df_jobs.info())
    # print(df_jobs.head())

    # Inicia la migracion de los archivos leidos a la nueva base de datos
    df_hired_employees.to_sql('hired_employees', conn, if_exists='replace', index=False)
    df_departments.to_sql('departments', conn, if_exists='replace', index=False)
    df_jobs.to_sql('jobs', conn, if_exists='replace', index=False)

    # Finaliza la migracion cerrando la conexion a la BD
    conn.close()


def test_moved_files_in_bd(test_table):
    logging.info('Ejecuta: get_db_connection')
    conn = get_db_connection()
    cursor = conn.cursor()
    test_table = test_table
    logging.info(f'Inicia el testeo de la tabla {test_table}')
    cursor.execute(f'SELECT id FROM {test_table} WHERE id = 1')
    if cursor.fetchone():
        logging.info(f"La tabla {test_table} existe")
        # print(cursor.fetchall())
    else:
        logging.info(f"La tabla {test_table} no existe")

    logging.info('Inicia el testeo de listado de tablas')
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';") 
    tables = cursor.fetchall()
    tables = [table[0] for table in tables]
    table = ','.join(tables)
    logging.info(table)
    cursor.close()
    conn.close()
    # print(conn)


if __name__ == "__main__":
    create_data_base()
    move_files_to_db()
    test_moved_files_in_bd('hired_employees')