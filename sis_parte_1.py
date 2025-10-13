# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pendulum
from airflow.utils.task_group import TaskGroup

# Importaciones locales
from app.sis.common import get_variables, get_mongo_collection_minio, insert_sql_from_minio, insert_sql_from_minio_areas
from app.sql_queries import truncate_sql_cascade

# Llamar a la función y obtener las variables
tabla = "tablas-parte-1"
config_variables = get_variables(params=[tabla,"mongo_connection","db_connection_id", "minio_connection_id"])

# Acceder a las variables
default_args = config_variables["default_args"]
mongo_connection_id, mongo_database_name = config_variables["mongo_connection"].values()
db_connection_id = config_variables["db_connection_id"]
minio_connection_id = config_variables["minio_connection_id"]
tables = config_variables[tabla]
batch_size = config_variables["batch_size"]
schedule = config_variables["schedule"]

# Definición del DAG
with DAG(
    dag_id="sis-parte-01",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "mongo", "datos"],
    doc_md="""### DAG: sis-parte-01
    Este DAG realiza la extracción de datos de colecciones en MongoDB, trunca tablas correspondientes en MySQL y luego inserta los datos desde Minio en las tablas correspondientes. Los pasos incluyen:
    
    - Obtener colecciones de MongoDB y almacenarlas en Minio.
    - Truncar las tablas en MySQL relacionadas.
    - Insertar los datos desde Minio en las tablas truncadas.

    El DAG está organizado en tareas secuenciales dentro de un grupo de tareas llamado `sis_parte_1` para cada colección de MongoDB.
    """
) as dag:
    
    clave = "areas"
    areas = tables.pop(clave)
    
    db_table_name, db_table_columns = areas
    
    get_mongo_collection_task = PythonOperator(
        task_id=f'get_mongo_collection_task_{clave}',
        python_callable=get_mongo_collection_minio,
        op_kwargs={
            'collection': clave, 
            'connection_id': mongo_connection_id, 
            'database_name': mongo_database_name, 
            'minio_id': minio_connection_id, 
            'bucket_name': "airflow-sis"
        },
        doc_md=f"""### Obtener colección MongoDB: {clave}
            Esta tarea extrae los datos de la colección `{clave}` en MongoDB y los almacena en Minio para su posterior procesamiento.
            """
    )

    truncate_table_task = SQLExecuteQueryOperator(
        task_id=f'truncate_table_task_{db_table_name}',
        conn_id=db_connection_id,
        sql=truncate_sql_cascade,
        params={'table_name': db_table_name},
        doc_md=f"""### Truncar tabla MySQL: {db_table_name}
            Esta tarea trunca la tabla `{db_table_name}` en MySQL utilizando una consulta SQL predefinida para asegurar que no haya registros previos antes de insertar nuevos datos.
            """
    )
    
    insert_task = PythonOperator(
        task_id=f'insert_table_task_{clave}',
        python_callable=insert_sql_from_minio_areas,
        op_kwargs={
            'minio_id': minio_connection_id, 
            'bucket_name': "airflow-sis", 
            'collection': clave, 
            'columns': db_table_columns,
            'table': db_table_name, 
            'connection_id': db_connection_id, 
            'batch_size': batch_size
        },
        doc_md=f"""### Insertar datos en MySQL: {db_table_name}
            Esta tarea inserta los datos desde Minio en la tabla `{db_table_name}` en MySQL. Los datos se insertan en lotes utilizando el tamaño de lote configurado (`batch_size`).
            """
    )
    # Dependencias entre las tareas
    [get_mongo_collection_task, truncate_table_task] >> insert_task
    
    with TaskGroup('sis_parte_1') as part_1_group:
        for mongo_collection_name, db_table_info in tables.items():
            db_table_name = db_table_info[0]
            db_table_columns = db_table_info[1]

            get_mongo_collection_task = PythonOperator(
                task_id=f'get_mongo_collection_task_{mongo_collection_name}',
                python_callable=get_mongo_collection_minio,
                op_kwargs={
                    'collection': mongo_collection_name, 
                    'connection_id': mongo_connection_id, 
                    'database_name': mongo_database_name, 
                    'minio_id': minio_connection_id, 
                    'bucket_name': "airflow-sis"
                },
                doc_md=f"""### Obtener colección MongoDB: {mongo_collection_name}
                Esta tarea extrae los datos de la colección `{mongo_collection_name}` en MongoDB y los almacena en Minio para su posterior procesamiento.
                """
            )
            
            truncate_table_task = SQLExecuteQueryOperator(
                task_id=f'truncate_table_task_{db_table_name}',
                conn_id=db_connection_id,
                sql=truncate_sql_cascade,
                params={'table_name': db_table_name},
                doc_md=f"""### Truncar tabla MySQL: {db_table_name}
                Esta tarea trunca la tabla `{db_table_name}` en MySQL utilizando una consulta SQL predefinida para asegurar que no haya registros previos antes de insertar nuevos datos.
                """
            )

            insert_task = PythonOperator(
                task_id=f'insert_table_task_{mongo_collection_name}',
                python_callable=insert_sql_from_minio,
                op_kwargs={
                    'minio_id': minio_connection_id, 
                    'bucket_name': "airflow-sis", 
                    'collection': mongo_collection_name, 
                    'columns': db_table_columns,
                    'table': db_table_name, 
                    'connection_id': db_connection_id, 
                    'batch_size': batch_size
                },
                doc_md=f"""### Insertar datos en MySQL: {db_table_name}
                Esta tarea inserta los datos desde Minio en la tabla `{db_table_name}` en MySQL. Los datos se insertan en lotes utilizando el tamaño de lote configurado (`batch_size`).
                """
            )

            # Dependencias entre las tareas
            [get_mongo_collection_task, truncate_table_task] >> insert_task
