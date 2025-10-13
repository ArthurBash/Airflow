# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from airflow.models.baseoperator import chain

# Importaciones locales
from app.sis.common import get_variables
from app.common.mongo_utils import get_mongo_collection
from app.common.minio_utils import store_in_minio

# Otras importaciones
import json
import logging


from app.common.parquet import extract_mongo_to_minio_parquet

from app.sis.extra_column import (generate_extra_parte_solicitudes,generate_extra_parte_responsables,
                                 generate_extra_parte_avances,generate_extra_parte_personasafectada,
                                 generate_extra_parte_solicitante)

# Llamar a la función y obtener las variables
tabla_names = ["solicitudes", "responsables", "avances", "personasafectada", "solicitante"]
config_variables = get_variables(params=tabla_names + ["mongo_connection", "minio_connection_id"])

# Acceder a las variables
default_args = config_variables["default_args"]
mongo_connection_id, mongo_database_name = config_variables["mongo_connection"].values()
minio_connection_id = config_variables["minio_connection_id"]
schedule = config_variables["schedule"]

# Obtener los nombres de las colecciones de MongoDB
mongo_collection_names = [next(iter(config_variables[tabla].items()))[0] for tabla in tabla_names]


extra_columns_funcs = {
    "peticions": generate_extra_parte_solicitudes,
    "responsables": generate_extra_parte_responsables,
    "avances": generate_extra_parte_avances,
    "personaafectada":generate_extra_parte_personasafectada,
    "solicitante":generate_extra_parte_solicitante
  
}



def get_and_store_mongo_to_minio(collection, connection_id, database_name, minio_id, bucket_name):
    """
    Obtiene una colección de MongoDB y la guarda en un archivo en Minio.

    Args:
        collection (str): El nombre de la colección en MongoDB.
        connection_id (str): El ID de la conexión de MongoDB.
        database_name (str): El nombre de la base de datos en MongoDB.
        minio_id (str): El ID de la conexión de Minio en Airflow.
        bucket_name (str): El nombre del bucket en Minio.

    Returns:
        None
    """
    # Obtener los documentos de MongoDB
    data = get_mongo_collection(collection, connection_id, database_name)
    
    # Almacenar en Minio
    store_in_minio(data,bucket_name, collection, minio_id )

# Definición del DAG
with DAG(
    dag_id="sis-get",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "mongo", "datos"],
    doc_md="""
    ## DAG `sis-get`
    Este DAG obtiene varias colecciones de MongoDB y las guarda en archivos en Minio.
    
    Las colecciones que se procesan incluyen `solicitudes`, `responsables`, `avances`, `personasafectada`, y `solicitante`.
    La tarea se ejecuta de manera periódica según el intervalo definido, y cada colección es procesada de forma independiente.
    
    ### Flujo del DAG:
    1. Se obtiene la colección de MongoDB.
    2. Se guarda la colección obtenida en un archivo dentro de Minio para su posterior procesamiento.
    """
) as dag:

  # Crear las tareas en un loop para evitar repetición
  tasks = []  # Lista para almacenar las tareas


  for i, collection_name in enumerate(mongo_collection_names, start=2):
      task = PythonOperator(
          task_id=f'get_mongo_collection_task_parte_{i}',
          python_callable=extract_mongo_to_minio_parquet,
          op_kwargs={
              'collection': collection_name, 
              'connection_id': mongo_connection_id, 
              'database_name': mongo_database_name, 
              'minio_id': minio_connection_id, 
              'bucket_name': "airflow-paquet",
              'object_name':f"{collection_name}/{collection_name}",
              'chuck_size': 50000,
              'extra_column':extra_columns_funcs.get(collection_name,None)
          },
          doc_md=f"""
          ## Tarea: Obtener colección `{collection_name}` de MongoDB y guardarla en Minio
          Esta tarea obtiene la colección `{collection_name}` desde la base de datos MongoDB y la almacena en Minio.
          El archivo se guarda en el bucket `airflow-sis` con el nombre correspondiente a la colección.
          """,
          dag=dag  # Asegurate de que el DAG esté definido
      )
      tasks.append(task)  # Agregar tarea a la lista
  
  # ✅ CREAR DEPENDENCIAS SECUENCIALES
  # Esto hace que task[0] >> task[1] >> task[2] >> task[3]...
  for i in range(len(tasks) - 1):
      tasks[i] >> tasks[i + 1]
