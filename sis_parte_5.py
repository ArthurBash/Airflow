# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pendulum

# Importaciones locales
# from app.sql_queries import truncate_sql_cascade

from app.sis.common import get_variables
from app.common.chuck import restore_parquet_to_postgres_optimized

# Llamar a la función y obtener las variables
tabla = "personasafectada"
config_variables = get_variables(params=[tabla, "db_connection_id", "minio_connection_id"])

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
minio_connection_id = config_variables["minio_connection_id"]
table = config_variables[tabla]
batch_size = config_variables["batch_size"]
schedule = config_variables["schedule"]

# Obtenemos el nombre de la colección de MongoDB, el nombre de la tabla de db y su estructura (columnas)
mongo_collection_name, db_table_info = next(iter(table.items()))
db_table_name, db_table_columns = db_table_info

# def generate_extra_columns(documents):
#     """
#     Genera columnas adicionales para la inserción en MySQL desde un documento de MongoDB.

#     Args:
#         documents (dict): Documento de MongoDB.

#     Returns:
#         dict: Diccionario con las columnas adicionales como 'provincia', 'localidad', 'direccion',
#         'generoId', 'nacionalidad', 'edad', 'ocupacion', 'infoDemograficaId', 'updated_at', y 'created_at'.
#     """
#     return {
#         "provincia": documents.get("domicilio", {}).get("provincia", None),
#         "localidad": documents.get("domicilio", {}).get("localidad", None),
#         "direccion": documents.get("domicilio", {}).get("direccion", None),
#         "generoId": documents.get("infoDemografica", {}).get("generoId", None),
#         "nacionalidad": documents.get("infoDemografica", {}).get("nacionalidad", None),
#         "edad": documents.get("infoDemografica", {}).get("edad", None),
#         "ocupacion": documents.get("infoDemografica", {}).get("ocupacion", None),
#         "infoDemograficaId": documents.get("infoDemografica", {}).get("_id", None),
#         "mongo_updated_at": parse_date(documents.get("updatedAt")),
#         "mongo_created_at": parse_date(documents.get("createdAt"))
#     }

# Definición del DAG
with DAG(
    dag_id="sis-parte-05",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "mongo", "datos"],
    doc_md="""
    ### DAG sis-parte-05
    Este DAG procesa los datos de la colección 'personasafectada' de MongoDB, realizando un truncado de la tabla en MySQL y 
    utilizando Minio como almacenamiento temporal para luego insertar los datos transformados en la base de datos.
    """
) as dag:

    insert_task = PythonOperator(
        task_id='insert_table_task',
        python_callable=restore_parquet_to_postgres_optimized,
        op_kwargs={
            'minio_id': minio_connection_id,
            'bucket_name': "airflow-paquet",
            'path_pattern': f"{mongo_collection_name}/{mongo_collection_name}",
            'columns': db_table_columns,
            'table': db_table_name,
            'connection_id': db_connection_id,
            'batch_size': batch_size,
            'read_chunk_size': batch_size
        },
        doc_md=f"""### Insertar datos en Postgres: {db_table_name}
        Inserta los datos desde parquet en Minio en la tabla `{db_table_name}` (personas afectadas) en Postgres.
        """
    )

    # Definición del flujo del DAG
    insert_task
