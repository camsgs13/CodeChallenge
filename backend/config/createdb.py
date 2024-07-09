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

os.system('cls')

# Crea la conexion con la base de datos a usar
conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Configura la ruta donde se encuentran los archivos a migrar a la nueva base de datos
SRC_DIR = 'backend/src'
os.chdir(SRC_DIR)

# Inicia la lectura de los archivos
# Archivo hired_employees.xlsx
columns = ['id','name','datetime','department_id','job_id']
data_types = {'id':'Int64',
              'name':str,
              'datetime':str,
              'department_id':'Int64',
              'job_id':'Int64'}
df_hired_employees = pd.read_excel('hired_employees.xlsx',names=columns, dtype=data_types,header=None)
print(df_hired_employees.info())
print(df_hired_employees.head())

# Archivo departments.xlsx
columns = ['id','department']
data_types = {'id':'Int64',
              'department':str}
df_departments = pd.read_excel('departments.xlsx',names=columns, dtype=data_types,header=None)
print(df_departments.info())
print(df_departments.head())

# Archivo jobs.xlsx
columns = ['id','job']
data_types = {'id':'Int64',
              'job':str}
df_jobs = pd.read_excel('jobs.xlsx',names=columns, dtype=data_types,header=None)
print(df_jobs.info())
print(df_jobs.head())

# Inicia la migracion de los archivos leidos a la nueva base de datos
# df_hired_employees.to_sql('hired_employees', conn, if_exists='replace', index=False)
# df_departments.to_sql('departments', conn, if_exists='replace', index=False)
# df_jobs.to_sql('jobs', conn, if_exists='replace', index=False)
# conn.close()

test_table = 'hired_employees'
cursor.execute(f'SELECT id FROM {test_table} WHERE id = 1')
if cursor.fetchone():
    print(f"La tabla {test_table} existe")
else:
    print(f"La tabla {test_table} no existe")

