# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

# Importaciones locales
from app.sis.common import (
  get_variables,
  parse_date, parse_time,
  insert_sql_from_minio,
  insert_metadatos_from_minio
)

from app.common.chuck import restore_parquet_to_postgres_optimized,insert_metadatos_from_parquet_streaming

# Llamar a la función y obtener las variables
tabla_solicitudes = "solicitudes"
tabla_solicitudesmetadatos = "solicitudmetadatos"

config_variables = get_variables(
  params=[tabla_solicitudes,tabla_solicitudesmetadatos,"db_connection_id","minio_connection_id"]
)

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
minio_connection_id = config_variables["minio_connection_id"]
batch_size = config_variables["batch_size"]
schedule = config_variables["schedule"]

solicitudes_config = config_variables[tabla_solicitudes]
solicitudes_metadatos_config = config_variables[tabla_solicitudesmetadatos]

# Extraer la colección y la estructura de la tabla de solicitudes
mongo_collection_solicitudes, db_table_solicitudes_info = next(iter(solicitudes_config.items()))
db_table_name_solicitudes, db_table_columns_solicitudes = db_table_solicitudes_info

# Extraer la colección y la estructura de la tabla de metadatos de solicitudes
mongo_collection_solicitudes_metadatos, db_table_solicitudes_metadatos_info = next(iter(solicitudes_metadatos_config.items()))
db_table_name_solicitudes_metadatos, db_table_columns_solicitudes_metadatos = db_table_solicitudes_metadatos_info

def generate_extra_columns(documents):
    """
    Genera un diccionario de columnas adicionales a partir de un documento de MongoDB para inserción en MySQL.

    Args:
        documents (dict): Documento de MongoDB con datos de la solicitud.

    Returns:
        dict: Diccionario con las columnas generadas para la base de datos, como fechas, IDs y detalles de lugar.
    """
    return {
        "fecha": parse_date(documents.get("fecha")),
        "fechaIngreso": parse_date(documents.get("fechaIngreso")),
        "fechaUltimoAvance": parse_date(documents.get("fechaUltimoAvance")),
        "afectadas_ids": ",".join(documents.get("afectadas", [])),
        "responsables_ids": ",".join(documents.get("responsables", [])),
        "tipolugar_id": documents.get("lugar", {}).get("tipo"),
        "area_id": documents.get("area"),
        "canal_id": documents.get("canal"),
        "subtipo_id": documents.get("tipo"),
        "estado_id": documents.get("estado"),
        "lugar_detalle": documents.get("lugar", {}).get("detalle"),
        "lugar": documents.get("lugar", {}).get("nombre"),
        "solicitante_id": documents.get("solicitante"),
        "usuario_id": documents.get("usuario"),
        "hora": parse_time(documents.get("hora")),
        "mongo_updated_at": parse_date(documents.get("updatedAt")),
        "mongo_created_at": parse_date(documents.get("createdAt")),
    }

# Definición del DAG
with DAG(
    dag_id="sis-parte-02",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "mongo", "datos"],
    doc_md="""### DAG: sis-parte-02
    Este DAG realiza la extracción de datos desde la colección de MongoDB `solicitudes`,y luego actualiza  los datos procesados en la tabla correspondiente. Los pasos incluyen:
     
    - Obtener los datos desde la colección de MongoDB `solicitudes`, procesarlos y almacenarlos en Minio.
    - Insertar los datos procesados desde Minio en la tabla de Postgres.

    El procesamiento incluye la generación de columnas adicionales basadas en el contenido de los documentos de MongoDB, como fechas y IDs de las afectadas, responsables, entre otros.
    """
) as dag:
  
    insert_task = PythonOperator(
        task_id='insert_table_task',
        python_callable=restore_parquet_to_postgres_optimized,
        op_kwargs={
            'minio_id': minio_connection_id, 
            'bucket_name': "airflow-paquet", 
            'path_pattern': f"{mongo_collection_solicitudes}/{mongo_collection_solicitudes}", 
            'columns': db_table_columns_solicitudes, 
            'table': db_table_name_solicitudes, 
            'connection_id': db_connection_id, 
            'batch_size': batch_size,
            'read_chunk_size': batch_size
        },
        doc_md=f"""### Insertar datos en Postgres: {db_table_name_solicitudes}
        Esta tarea inserta los datos desde Minio en la tabla `{db_table_name_solicitudes}` en Postgres. Los datos son procesados para agregar columnas adicionales generadas mediante la función `generate_extra_columns`, que transforma las fechas y genera listas de IDs. Los datos se insertan en lotes utilizando el tamaño de lote configurado (`batch_size`).
        """
    )


    insert_solicitudes_metadatos_task = PythonOperator(
      task_id='insert_solicitudes_metadatos_task',
      python_callable=insert_metadatos_from_parquet_streaming,
      op_kwargs={
        'minio_id': minio_connection_id,
        'bucket_name': "airflow-paquet",
        'path_pattern': f"{mongo_collection_solicitudes}/{mongo_collection_solicitudes}",
        'columns': db_table_columns_solicitudes_metadatos,
        'table': db_table_name_solicitudes_metadatos,
        'connection_id': db_connection_id,
        'batch_size': batch_size,
        "reference_key": "solicitud_id",
        "read_chunk_size":50000
      },
      doc_md="Inserta los metadatos transformados en Postgres."
    )

    # Dependencias entre las tareas
    insert_task >> insert_solicitudes_metadatos_task
