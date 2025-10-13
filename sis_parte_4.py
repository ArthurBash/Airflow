# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pendulum

# Importaciones locales
from app.sql_queries import truncate_sql


from app.sis.common import get_variables
from app.common.chuck import restore_parquet_to_postgres_optimized, insert_metadatos_from_parquet_streaming


# Llamar a la función y obtener las variables
tabla_avances = "avances"
tabla_metadatos = "metadatos"

config_variables = get_variables(params=[tabla_avances, tabla_metadatos, "db_connection_id", "minio_connection_id"])

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
minio_connection_id = config_variables["minio_connection_id"]

# Obtenemos la estructura de las tablas
table_avances = config_variables[tabla_avances]
table_metadatos = config_variables[tabla_metadatos]

batch_size = config_variables["batch_size"]
schedule = config_variables["schedule"]

# Obtenemos los nombres de las colecciones de MongoDB, los nombres de las tablas de db y sus estructuras (columnas)
mongo_collection_name_avances, db_table_info_avances = next(iter(table_avances.items()))
db_table_name_avances, db_table_columns_avances = db_table_info_avances

mongo_collection_name_metadatos, db_table_info_metadatos = next(iter(table_metadatos.items()))
db_table_name_metadatos, db_table_columns_metadatos = db_table_info_metadatos

# def generate_extra_columns(documents):
#     """
#     Genera columnas adicionales para los avances a partir de documentos de MongoDB.
#     """
#     return {
#         "fecha": parse_date(documents.get("fecha")),
#         "cantidad_metadatos": str(len(documents.get('metadatos', []))),
#         "solicitud_id": documents.get("peticion", None),
#         "usuario_id": documents.get("usuario", None)
#     }

# #NO SE USA!
# def process_metadatos_from_input(input_data):
#     """
#     Procesa los datos de 'metadatos' desde el input JSON para transformarlos a un formato adecuado para la inserción en la base de datos.

#     Args:
#         input_data (list): Lista de entradas de datos JSON, cada una conteniendo los metadatos.

#     Returns:
#         list: Lista de metadatos transformados.
#     """
#     metadatos = []

#     for item in input_data:
#         if 'metadatos' in item['json']:
#             for metadato in item['json']['metadatos']:
#                 nuevo_metadato = {
#                     'avance_id': item['json']['_id'],
#                     'tipometadato_id': metadato['tipo'],
#                     'detalle': metadato['detalle'],
#                     'fecha': metadato['fecha'][:10] if metadato['fecha'] else None
#                 }
#                 metadatos.append({'json': nuevo_metadato})

#     return metadatos


# Definición del DAG
with DAG(
    dag_id="sis-parte-04",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "mongo", "datos"],
    doc_md="""
    ### DAG sis-parte-04
    Este DAG extrae datos de las colecciones de MongoDB 'avances' y 'metadatos' y los inserta en las tablas MySQL correspondientes.
    Utiliza Minio como almacenamiento temporal y realiza un truncado en cascada antes de insertar los datos nuevos en ambas tablas.
    """
) as dag:

    # Inserción de avances
    insert_avances_task = PythonOperator(
        task_id='insert_avances_task',
        python_callable=restore_parquet_to_postgres_optimized,
        op_kwargs={
            'minio_id': minio_connection_id,
            'bucket_name': "airflow-paquet",
            'path_pattern': f"{mongo_collection_name_avances}/{mongo_collection_name_avances}",
            'columns': db_table_columns_avances,
            'table': db_table_name_avances,
            'connection_id': db_connection_id,
            'batch_size': batch_size,
            'read_chunk_size': batch_size
        },
        doc_md="Inserta los avances transformados en Postgres."
    )

    insert_avances_metadatos_task = PythonOperator(
        task_id='insert_avances_metadatos_task',
        python_callable=insert_metadatos_from_parquet_streaming,
        op_kwargs={
            'minio_id': minio_connection_id,
            'bucket_name': "airflow-paquet",
            'path_pattern': f"{mongo_collection_name_avances}/{mongo_collection_name_avances}",
            'columns': db_table_columns_metadatos,
            'table': db_table_name_metadatos,
            'connection_id': db_connection_id,
            'batch_size': batch_size,
            'reference_key': "avance_id",
            'read_chunk_size': batch_size
        },
        doc_md="Inserta los metadatos desde parquet en Postgres."
    )

    # truncate_metadatos_table_task = SQLExecuteQueryOperator(
    #     task_id=f'truncate_postgres_table_task_metadatos',
    #     conn_id='postgres_connection',
    #     sql=truncate_sql,
    #         params={
    #             'table_name': db_table_name_metadatos,
    #         },
    # )

    # Secuencia de tareas para avances y metadatos
    insert_avances_task  >> insert_avances_metadatos_task
