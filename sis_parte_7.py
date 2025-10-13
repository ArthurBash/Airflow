# Importaciones de Airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pendulum

# Importaciones locales
from app.sis.common import get_variables, get_db_hook
from app.sql_queries import (
    sql_ranges,
    sql_ranges_infantil,
    sql_update_fields,
    sql_update_single,
    sql_update_single_solicitud,
)

# Otras importaciones
import json
import logging
from io import BytesIO

# Llamar a la función y obtener las variables
config_variables = get_variables(
    params=[
        "db_connection_id",
        "minio_connection_id",
        "personasafectada",
        "solicitudes",
        "responsables",
        "tipoidentificacion",
        "generos",
    ]
)

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
afectada_table_name = next(iter(config_variables["personasafectada"].values()))[0]
solicitudes_table_name = next(iter(config_variables["solicitudes"].values()))[0]
responsables_table_name = next(iter(config_variables["responsables"].values()))[0]
tipoidentificacion_table_name = next(
    iter(config_variables["tipoidentificacion"].values())
)[0]
generos_table_name = next(iter(config_variables["generos"].values()))[0]
minio_connection_id = config_variables["minio_connection_id"]
schedule = config_variables["schedule"]

def get_table_data_minio(table_origin, table, field, connection_id, minio_id, bucket_name):
    """
    Esta función extrae datos desde una tabla en una base de datos, los transforma en formato JSON,
    y los guarda en un archivo en Minio.

    :param table_origin: Nombre de la tabla en la base de datos.
    :param table: Nombre de la tabla de destino.
    :param field: Campo de la tabla que contiene los datos a extraer.
    :param connection_id: ID de la conexión a la base de datos.
    :param minio_id: ID de la conexión a Minio.
    :param bucket_name: Nombre del bucket de Minio donde almacenar los datos.
    """
    # Obtener el hook de la base de datos
    db_hook = get_db_hook(connection_id)
    
    # Consulta SQL para obtener los datos
    sql_get = f"""
        SELECT _id, {field}, '{table}' as tabla 
        FROM {table_origin}
        WHERE (
            {field} IS NOT NULL AND
            {field} <> '' AND
            (LENGTH({field}) - LENGTH(REPLACE({field}, ',', ''))) > 0
        );
    """
    
    try:
        # Obtener los registros de la base de datos
        results = db_hook.get_records(sql=sql_get)
    except Exception as e:
        logging.error(f'Error al ejecutar la consulta en la BD. Error completo: "{e}"')
        raise

    # Acumular los datos en memoria
    try:
        data_list = []
        for row in results:
            data = {"_id": row[0], field: row[1], "tabla": row[2]}
            data_list.append(data)
        
        # Convertir los datos a JSON y escribirlos todos juntos
        file_obj = BytesIO()
        file_obj.write("\n".join([json.dumps(item) for item in data_list]).encode('utf-8'))
        file_obj.seek(0)
        
        # Inicializar el hook de Minio
        s3_hook = S3Hook(aws_conn_id=minio_id)
        
        # Cargar el archivo en Minio
        s3_hook.load_file_obj(
            file_obj, key=f"{table}_data", bucket_name=bucket_name, replace=True
        )
        logging.info(f"Archivo con datos de {table} cargado correctamente en Minio.")
    
    except Exception as e:
        logging.error(f'Error al procesar datos para Minio. Error completo: "{e}"')
        raise
        
def create_diccionario_minio(table, field, connection_id, minio_id, bucket_name):
    """
    Esta función descarga un archivo JSON desde Minio y actualiza registros en la base de datos 
    con los valores extraídos de dicho archivo.

    :param table: Nombre de la tabla de destino.
    :param field: Campo a actualizar en la base de datos.
    :param connection_id: ID de la conexión a la base de datos.
    :param minio_id: ID de la conexión a Minio.
    :param bucket_name: Nombre del bucket en Minio.
    """
    # Inicializar el hook de Minio
    s3_hook = S3Hook(aws_conn_id=minio_id)

    # Descargar el archivo desde Minio
    try:
        file_obj = s3_hook.read_key(key=f"{table}_data", bucket_name=bucket_name)
        
    except Exception as e:
        logging.error(f"Error al leer el archivo desde Minio. Error completo: '{e}'")
        raise

    # Conectar a la base de datos
    db_hook = get_db_hook(connection_id)
    try:
        # Procesar línea por línea
        for line in file_obj.splitlines():
            try:
                # Convertir la línea a un diccionario
                registro = json.loads(line)
                ids = registro[field].split(",")  # Suponemos que los IDs están separados por coma
                for each in ids:
                    sql_query = f"""
                        UPDATE {registro['tabla']} 
                        SET solicitud_id='{registro["_id"]}' 
                        WHERE _id='{each}'
                    """
                    # Ejecutar la actualización directamente sin almacenarlo en una lista
                    db_hook.run(sql_query)
                
            except json.JSONDecodeError as e:
                logging.error(f"Error al decodificar JSON en la línea: {line}. Error: {e}")
                continue  # Saltar esta línea si hay un error en el JSON

        db_hook.get_conn().commit()  # Confirmar cambios en la base de datos

    except Exception as e:
        logging.error(f"Error al ejecutar la consulta en la BD. Error completo: '{e}'")
        db_hook.get_conn().rollback()  # Revertir si ocurre algún error
        raise

# Definición del DAG
with DAG(
    dag_id="sis-parte-07",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "datos"],
    doc_md="""### DAG: sis-parte-07
    Este DAG realiza la parte 7 del proceso SIS, que incluye la extracción y actualización de datos en MySQL a partir de varias colecciones en MongoDB. Los pasos incluyen:

    - Obtener y procesar datos desde MongoDB.
    - Realizar actualizaciones y transformaciones en MySQL.
    - Utilizar Minio para almacenar y recuperar los datos necesarios para las actualizaciones.

    El DAG está organizado en tareas secuenciales y se utiliza para realizar múltiples operaciones de actualización de datos de forma eficiente.
    """
) as dag:

    get_afectada = PythonOperator(
        task_id='get_afectada_task',
        python_callable=get_table_data_minio,
        op_kwargs={
            'table_origin': solicitudes_table_name,
            'table': afectada_table_name,
            'field':'afectadas_ids',
            'connection_id': db_connection_id,
            'minio_id': minio_connection_id,
            'bucket_name': "airflow-sis",
        },
        doc_md="""### Obtener datos de 'afectadas' desde Minio
        Esta tarea extrae los IDs de personas afectadas desde la tabla de solicitudes y los almacena en Minio para su posterior procesamiento.
        """
    )

    get_responsable = PythonOperator(
        task_id='get_responsable_task',
        python_callable=get_table_data_minio,
        op_kwargs={
            'table_origin': solicitudes_table_name,
            'table': responsables_table_name,
            'field': "responsables_ids",
            'connection_id': db_connection_id,
            'minio_id': minio_connection_id,
            'bucket_name': "airflow-sis",
        },
        doc_md="""### Obtener datos de 'responsables' desde Minio
        Esta tarea extrae los IDs de responsables desde la tabla de solicitudes y los almacena en Minio para su posterior procesamiento.
        """
    )

    update_multiple_afectada = PythonOperator(
        task_id='update_multiple_afectada',
        python_callable=create_diccionario_minio,
        op_kwargs={
            'table': afectada_table_name,
            'tabla_nueva': "listadeafectados",
            'field': 'afectadas_ids',
            'connection_id': db_connection_id,
            'minio_id': minio_connection_id,
            'bucket_name': "airflow-sis",
        },
        doc_md="""### Actualizar registros de 'afectadas' en la base de datos
        Esta tarea actualiza los registros de personas afectadas en la base de datos, asignando el ID de solicitud correspondiente, basado en los datos almacenados en Minio.
        """
    )

    update_multiple_responsable = PythonOperator(
        task_id='update_multiple_responsable',
        python_callable=create_diccionario_minio,
        op_kwargs={
            'table': responsables_table_name,
            'tabla_nueva': "listaderesponsables",
            'field': "responsables_ids",
            'connection_id': db_connection_id,
            'minio_id': minio_connection_id,
            'bucket_name': "airflow-sis",
        },
        doc_md="""### Actualizar registros de 'responsables' en la base de datos
        Esta tarea actualiza los registros de responsables en la base de datos, asignando el ID de solicitud correspondiente, basado en los datos almacenados en Minio.
        """
    )

    update_single_responsable_solicitud = SQLExecuteQueryOperator(
        task_id='update_single_responsable_solicitud',
        conn_id=db_connection_id,
        sql=sql_update_single_solicitud,
        params={
            'table_name': solicitudes_table_name,
            'campo': "responsables_ids",
            'campo_single': "responsable_id_single",
        },
        doc_md="""### Actualizar registros de 'responsable' en la tabla 'solicitudes'
        Esta tarea actualiza los registros de la tabla `solicitudes`, asignando los IDs de responsables correspondientes.

        #### Parámetros:
        - `table_name`: Nombre de la tabla `solicitudes` que será actualizada.
        - `campo`: El campo que contiene los IDs de los responsables.
        - `campo_single`: El campo donde se asigna el `responsable_id_single`.

        #### Descripción:
        Esta tarea asigna el `responsable_id_single` al campo correspondiente en la tabla `solicitudes`, usando los datos de la tabla `responsables`.
        """
    )

    update_single_afectada_solicitud = SQLExecuteQueryOperator(
        task_id="update_single_afectada_solicitud",
        sql=sql_update_single_solicitud,
        conn_id=db_connection_id,
        params={
            'table_name': solicitudes_table_name,
            'campo': "afectadas_ids",
            'campo_single': "afectada_id_single",
        },
        hook_params = {
            "options": "-c statement_timeout=300000ms",
            "connect_timeout": 30
        },
        doc_md="""### Actualizar registros de 'afectada' en la tabla 'solicitudes'
        Esta tarea actualiza los registros de la tabla `solicitudes`, asignando los IDs de afectadas correspondientes.

        #### Parámetros:
        - `table_name`: Nombre de la tabla `solicitudes` que será actualizada.
        - `campo`: El campo que contiene los IDs de las afectadas.
        - `campo_single`: El campo donde se asigna el `afectada_id_single`.

        #### Descripción:
        Esta tarea asigna el `afectada_id_single` al campo correspondiente en la tabla `solicitudes`, usando los datos de la tabla `afectadas`.
        """
    )

    update_single_afectada = SQLExecuteQueryOperator(
        task_id='update_single_afectada',
        conn_id=db_connection_id,
        sql=sql_update_single,
        params={
            'table_name': afectada_table_name,
            'table_solicitudes': solicitudes_table_name,
            'campo': "solicitud_id",
            'campo_single': "afectada_id_single",
        },
        doc_md="""### Actualizar registros de 'afectada' en la base de datos
        Esta tarea actualiza los registros de afectadas en la base de datos, asignando el ID de solicitud correspondiente, basado en los datos de la tabla `solicitudes`.

        #### Parámetros:
        - `table_name`: Nombre de la tabla `afectadas` que será actualizada.
        - `table_solicitudes`: Nombre de la tabla `solicitudes` desde donde se extraen los IDs de las solicitudes.
        - `campo`: El campo donde se asigna el `solicitud_id`.
        - `campo_single`: El campo donde se asigna el `afectada_id_single`.

        #### Descripción:
        Esta tarea asigna el `solicitud_id` al campo correspondiente en la tabla `afectadas`, usando los datos de la tabla `solicitudes`.
        """
    )

    update_single_responsable = SQLExecuteQueryOperator(
        task_id='update_single_responsable',
        conn_id=db_connection_id,
        sql=sql_update_single,
        params={
            'table_name': responsables_table_name,
            'table_solicitudes': solicitudes_table_name,
            'campo': "solicitud_id",
            'campo_single': "responsable_id_single",
        },
        doc_md="""### Actualizar registros de 'responsable' en la base de datos
        Esta tarea actualiza los registros de responsables en la base de datos, asignando el ID de solicitud correspondiente, basado en los datos de la tabla `solicitudes`.

        #### Parámetros:
        - `table_name`: Nombre de la tabla `responsables` que será actualizada.
        - `table_solicitudes`: Nombre de la tabla `solicitudes` desde donde se extraen los IDs de las solicitudes.
        - `campo`: El campo donde se asigna el `solicitud_id`.
        - `campo_single`: El campo donde se asigna el `responsable_id_single`.

        #### Descripción:
        Esta tarea asigna el `solicitud_id` al campo correspondiente en la tabla `responsables`, usando los datos de la tabla `solicitudes`.
        """
    )

    update_fields = SQLExecuteQueryOperator(
        task_id="update_fields",
        conn_id=db_connection_id,
        sql=sql_update_fields,
        params={
            'table_name': afectada_table_name,
            'tipoidentificacion_table_name': tipoidentificacion_table_name,
            'generos_table_name': generos_table_name,
        },
        doc_md="""### Actualizar campos adicionales en la tabla 'afectada'
        Esta tarea actualiza campos adicionales en la tabla `afectadas`, usando datos de otras tablas relacionadas como `tipoidentificacion` y `generos`.

        #### Parámetros:
        - `table_name`: Nombre de la tabla `afectadas` que será actualizada.
        - `tipoidentificacion_table_name`: Nombre de la tabla `tipoidentificacion` que contiene los datos relacionados.
        - `generos_table_name`: Nombre de la tabla `generos` que contiene los datos relacionados.

        #### Descripción:
        Esta tarea actualiza los campos adicionales en la tabla `afectadas`, como el tipo de identificación y el género, usando los datos de las tablas relacionadas.
        """
    )

    update_ranges = SQLExecuteQueryOperator(
        task_id="update_ranges",
        conn_id=db_connection_id,
        sql=sql_ranges,
        params={'table_name': afectada_table_name},
        doc_md="""### Actualizar rangos en la tabla 'afectada'
        Esta tarea actualiza los rangos en la tabla `afectadas`, basado en los valores establecidos en el campo correspondiente.

        #### Parámetros:
        - `table_name`: Nombre de la tabla `afectadas` que será actualizada.

        #### Descripción:
        Esta tarea actualiza los rangos en la tabla `afectadas`, utilizando los valores definidos en los rangos correspondientes.
        """
    )

    update_ranges_infantil = SQLExecuteQueryOperator(
        task_id="update_ranges_infantil",
        conn_id=db_connection_id,
        sql=sql_ranges_infantil,
        params={'table_name': afectada_table_name},
        doc_md="""### Actualizar rangos infantiles en la tabla 'afectada'
        Esta tarea actualiza los rangos específicos para registros infantiles en la tabla `afectadas`.

        #### Parámetros:
        - `table_name`: Nombre de la tabla `afectadas` que será actualizada.

        #### Descripción:
        Esta tarea actualiza los rangos para los registros infantiles en la tabla `afectadas`, aplicando las condiciones de rango correspondientes.
        """
    )

    # Definir grupos de tasks
    get_table_tasks = [get_afectada, get_responsable]

    update_multiple_tasks = [update_multiple_afectada, update_multiple_responsable]

    update_single_tasks = [
        update_single_afectada_solicitud >> update_single_afectada,
        update_single_responsable_solicitud >> update_single_responsable,
    ]

    # Definir el flujo del DAG
    get_table_tasks >> update_single_afectada_solicitud >> update_single_afectada >> update_single_responsable_solicitud >> update_single_responsable >> update_multiple_tasks >> update_fields >> update_ranges >> update_ranges_infantil
