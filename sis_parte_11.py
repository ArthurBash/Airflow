# Importaciones de Airflow
from airflow import DAG
from datetime import datetime
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Importaciones locales
from app.sis.common import get_variables
from app.sql_queries import (
    truncate_sql,
    insert_beneficios_sql,
    update_beneficio_acta_sql,
    update_beneficio_fecha_sql,
    update_beneficio_numero_sql,
)

# Llamar a la función y obtener las variables
config_variables = get_variables(
    params=[
        "db_connection_id",
        "solicitudes",
        "avances",
        "metadatos",
        "tipometadato",
        "beneficios"
    ]
)

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
solicitudes_table_name = next(iter(config_variables["solicitudes"].values()))[0]
avances_table_name = next(iter(config_variables["avances"].values()))[0]
metadatos_table_name = next(iter(config_variables["metadatos"].values()))[0]
tipometadato_table_name = next(iter(config_variables["tipometadato"].values()))[0]
beneficios_table_name = next(iter(config_variables["beneficios"].values()))[0]
schedule = config_variables["schedule"]

# Definición del DAG
with DAG(
    dag_id="sis-parte-11",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "datos"],
    doc_md="""
    ### DAG: sis-parte-11
    Este DAG gestiona la tabla `beneficios`, realizando operaciones secuenciales 
    para truncar la tabla, insertar datos iniciales y luego actualizar 
    registros en campos específicos, como `acta`, `fecha`, y `número`. 
    Este flujo asegura que la información en `beneficios` esté sincronizada 
    y actualizada con los datos de otras tablas como `solicitudes`, `avances`, 
    `metadatos` y `tipometadato`.
    """
) as dag:
    
    truncate_db_table_task = SQLExecuteQueryOperator(
        task_id='truncate_db_table_task',
        conn_id=db_connection_id,
        sql=truncate_sql,
        params={
            'table_name': beneficios_table_name,
        },
        doc_md="""
        #### truncate_db_table_task
        Vacía la tabla `beneficios` para eliminar datos obsoletos o redundantes 
        antes de iniciar nuevas inserciones y actualizaciones. 
        Este paso asegura que solo se mantengan datos actuales en la tabla.
        """
    )
    
    insert_beneficios = SQLExecuteQueryOperator(
        task_id="insert_beneficio",
        conn_id=db_connection_id,
        sql=insert_beneficios_sql,
        params={
            'table_name': beneficios_table_name,
            'table_solicitudes': solicitudes_table_name,
            'table_avances': avances_table_name,
            'table_metadatos': metadatos_table_name,
            'table_tipometadato': tipometadato_table_name,
        },
        doc_md="""
        #### insert_beneficio
        Inserta registros iniciales en la tabla `beneficios` utilizando datos 
        de `solicitudes`, `avances`, `metadatos`, y `tipometadato`. 
        Esto permite construir la estructura base de `beneficios` con 
        la información consolidada de varias fuentes.
        """
    )
    
    update_beneficio_acta = SQLExecuteQueryOperator(
        task_id="update_beneficio",
        conn_id=db_connection_id,
        sql=update_beneficio_acta_sql,
        params={
            'table_name': beneficios_table_name,
            'table_solicitudes': solicitudes_table_name,
            'table_avances': avances_table_name,
            'table_metadatos': metadatos_table_name,
            'table_tipometadato': tipometadato_table_name,
        },
        doc_md="""
        #### update_beneficio_acta
        Actualiza el campo `acta` en la tabla `beneficios` con información 
        proveniente de `solicitudes`, `avances`, `metadatos`, y `tipometadato`. 
        Esta tarea asegura que los beneficios estén vinculados a sus correspondientes 
        registros de actas.
        """
    )
    
    update_beneficio_fecha = SQLExecuteQueryOperator(
        task_id="update_beneficio_fecha",
        conn_id=db_connection_id,
        sql=update_beneficio_fecha_sql,
        params={
            'table_name': beneficios_table_name,
            'table_solicitudes': solicitudes_table_name,
            'table_avances': avances_table_name,
            'table_metadatos': metadatos_table_name,
            'table_tipometadato': tipometadato_table_name,
        },
        doc_md="""
        #### update_beneficio_fecha
        Modifica el campo `fecha` en `beneficios`, incorporando la última información 
        de `solicitudes`, `avances`, `metadatos`, y `tipometadato`. 
        Esto garantiza que las fechas asociadas a cada beneficio estén actualizadas.
        """
    )

    update_beneficio_numero = SQLExecuteQueryOperator(
        task_id="update_beneficio_numero",
        conn_id=db_connection_id,
        sql=update_beneficio_numero_sql,
        params={
            'table_name': beneficios_table_name,
            'table_solicitudes': solicitudes_table_name,
            'table_avances': avances_table_name,
            'table_metadatos': metadatos_table_name,
            'table_tipometadato': tipometadato_table_name,
        },
        doc_md="""
        #### update_beneficio_numero
        Actualiza el campo `número` en la tabla `beneficios` basado en datos de 
        `solicitudes`, `avances`, `metadatos`, y `tipometadato`. 
        Esta tarea asegura que cada beneficio tenga un número de referencia preciso.
        """
    )

# Definición del flujo del DAG
truncate_db_table_task >> insert_beneficios >> update_beneficio_acta >> update_beneficio_fecha >> update_beneficio_numero
