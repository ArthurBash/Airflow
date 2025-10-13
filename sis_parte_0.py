# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pendulum

# Importaciones locales
from app.sis.common import get_variables, get_mongo_collection_minio, insert_sql_from_minio
from app.sql_queries import truncate_sql

# Llamar a la función y obtener las variables
tabla="usuarios"
config_variables = get_variables(params=[tabla,"mongo_connection","db_connection_id","minio_connection_id"])

# Acceder a las variables
default_args = config_variables["default_args"]
mongo_connection_id, mongo_database_name = config_variables["mongo_connection"].values()
db_connection_id = config_variables["db_connection_id"]
minio_connection_id = config_variables["minio_connection_id"]
table = config_variables[tabla]
batch_size = config_variables["batch_size"]
schedule = config_variables["schedule"]

# Obtener el nombre de la coleccion de MongoDB, el nombre de la tabla de db y su estructura (columnas)
mongo_collection_name, db_table_info = next(iter(table.items()))
db_table_name, db_table_columns = db_table_info

# Definición del DAG
with DAG(
    dag_id="sis-parte-00",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags = ["sis", "mongo", "datos"],
    doc_md = """
    ### DAG sis-parte-00
    Integra datos de MongoDB a MySQL utilizando Minio como almacenamiento temporal.
    """
) as dag:

    get_mongo_collection_task = PythonOperator(
        task_id='get_mongo_collection_task',
        python_callable=get_mongo_collection_minio,
        op_kwargs={
            'collection': mongo_collection_name,
            'connection_id': mongo_connection_id,
            'database_name': mongo_database_name,
            'minio_id': minio_connection_id,
            'bucket_name':"airflow-sis"
        },
    )
    get_mongo_collection_task.doc_md = """
    #### Tarea: Extracción de datos de MongoDB
    Extrae datos de la colección en MongoDB y los guarda en Minio.
    """

    truncate_table_task = SQLExecuteQueryOperator(
        task_id=f'truncate_table_task_{db_table_name}',
        conn_id=db_connection_id,
        sql=truncate_sql,
        params={
            'table_name': db_table_name,
        },
    )
    truncate_table_task.doc_md = """
    #### Tarea: Truncado de tabla en MySQL
    Vacía la tabla MySQL de destino para preparar la inserción de nuevos datos.
    """

    insert_task = PythonOperator(
        task_id='insert_table_task',
        python_callable=insert_sql_from_minio,
        op_kwargs={
            'minio_id': minio_connection_id, 
            'bucket_name': "airflow-sis", 
            'collection': mongo_collection_name, 
            'columns': db_table_columns, 
            'table': db_table_name, 
            'connection_id': db_connection_id, 
            'batch_size': batch_size,
        },
    )
    insert_task.doc_md = """
    #### Tarea: Inserción en MySQL
    Inserta los datos almacenados en Minio en la tabla MySQL, incluyendo las columnas adicionales.
    """
    
    [get_mongo_collection_task, truncate_table_task] >> insert_task
