# Importaciones de Airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pendulum

# Importaciones locales
from app.sis.common import get_variables
from app.sql_queries import (
    truncate_sql,
    insert_initial_intervenciones_poblacion_sql,
    insert_metadatos_intervenciones_poblacion_sql,
    update_usernames_intervenciones_poblacion_sql,
    update_tipo_subtipo_intervenciones_poblacion_sql
)

# Llamar a la función y obtener las variables
config_variables = get_variables(
    params=[
        "db_connection_id",
        "intervenciones_poblacion",
        "solicitudes",
        "avances",
        "usuarios",
        "metadatos",
        "tipometadato"
    ]
)

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
intervenciones_poblacion_table_name = next(iter(config_variables["intervenciones_poblacion"].values()))[0]
solicitudes_table_name = next(iter(config_variables["solicitudes"].values()))[0]
avances_table_name = next(iter(config_variables["avances"].values()))[0]
usuarios_table_name = next(iter(config_variables["usuarios"].values()))[0]
metadatos_table_name = next(iter(config_variables["metadatos"].values()))[0]
tipometadato_table_name = next(iter(config_variables["tipometadato"].values()))[0]
schedule = config_variables["schedule"]

# Definición del DAG
with DAG(
    dag_id="sis-parte-10",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags = ["sis", "datos"],
    doc_md="""
    ### DAG: sis-parte-10
    Este DAG realiza una serie de operaciones en la tabla `intervenciones_poblacion`, 
    empezando con el vaciado de la tabla y continuando con la inserción y actualización 
    de datos provenientes de varias tablas relacionadas (`solicitudes`, `avances`, `metadatos`, y `usuarios`). 
    Las tareas en este DAG aseguran la actualización de `intervenciones_poblacion` con los últimos datos disponibles.
    """
) as dag:
    
    truncate_db_table_task = SQLExecuteQueryOperator(
        task_id=f'truncate_db_table_task',
        conn_id=db_connection_id,
        sql=truncate_sql,
        params={
            'table_name': intervenciones_poblacion_table_name,
        },
        doc_md="""
        #### truncate_db_table_task
        Elimina todos los registros existentes en la tabla `intervenciones_poblacion`. 
        Esta tarea es esencial para preparar la tabla antes de realizar nuevas inserciones.
        """
    )
    
    insert_initial_intervenciones_poblacion = SQLExecuteQueryOperator(
        task_id="insert_initial_intervenciones_poblacion",
        conn_id=db_connection_id,
        sql=insert_initial_intervenciones_poblacion_sql,
        params={
            'table_name': intervenciones_poblacion_table_name,
            'table_solicitudes': solicitudes_table_name,
        },
        doc_md="""
        #### insert_initial_intervenciones_poblacion
        Inserta datos iniciales en `intervenciones_poblacion` a partir de la tabla `solicitudes`. 
        Esta tarea establece una base de datos preliminar para `intervenciones_poblacion`, 
        alineando las solicitudes relevantes.
        """
    )
    
    insert_metadatos_intervenciones_poblacion = SQLExecuteQueryOperator(
        task_id="insert_metadatos_intervenciones_poblacion",
        conn_id=db_connection_id,
        sql=insert_metadatos_intervenciones_poblacion_sql,
        params={
            'table_name': intervenciones_poblacion_table_name,
            'table_solicitudes': solicitudes_table_name,
            'table_avances': avances_table_name,
            'table_metadatos': metadatos_table_name,
            'table_tipometadato': tipometadato_table_name,
        },
        doc_md="""
        #### insert_metadatos_intervenciones_poblacion
        Completa la tabla `intervenciones_poblacion` con información adicional desde las tablas `avances`, `metadatos`, 
        y `tipometadato`, además de `solicitudes`. Este paso añade contexto y detalles específicos 
        de los metadatos a las intervenciones.
        """
    )
    
    update_usernames_intervenciones_poblacion = SQLExecuteQueryOperator(
        task_id="update_usernames_intervenciones_poblacion",
        conn_id=db_connection_id,
        sql=update_usernames_intervenciones_poblacion_sql,
        params={
            'table_name': intervenciones_poblacion_table_name,
            'table_usuarios': usuarios_table_name,
        },
        doc_md="""
        #### update_usernames_intervenciones_poblacion
        Actualiza los nombres de usuario en `intervenciones_poblacion` basándose en los datos de `usuarios`. 
        Esto asegura que cada intervención esté vinculada con el usuario correcto y refleje los datos actualizados.
        """
    )

    update_tipo_subtipo_intervenciones_poblacion = SQLExecuteQueryOperator(
        task_id="update_tipo_subtipo_intervenciones_poblacion",
        conn_id=db_connection_id,
        sql=update_tipo_subtipo_intervenciones_poblacion_sql,
        params={
            'table_intervenciones': intervenciones_poblacion_table_name,
            'table_solicitudes': solicitudes_table_name,
        },
        doc_md="""
        #### update_tipo_subtipo_intervenciones_poblacion
        Actualiza los valores de `tipo` y `subtipo` en `intervenciones_poblacion` basándose en `solicitudes`.
        Si `tipo_intervencion` es 'Informe', se asigna 'Aval de traslado' y se usa `detalle` como `subtipo`.
        De lo contrario, se copian los valores de `solicitudes`.
        """
  )

    
# Definición del flujo del DAG
truncate_db_table_task >> insert_initial_intervenciones_poblacion >> insert_metadatos_intervenciones_poblacion >> update_usernames_intervenciones_poblacion >> update_tipo_subtipo_intervenciones_poblacion
