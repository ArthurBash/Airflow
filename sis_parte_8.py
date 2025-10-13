
# Importaciones de Airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime,timedelta

import pendulum

# Importaciones locales
from app.sis.common import get_variables
from app.sql_queries import (
    truncate_sql,
    insert_sql_da_afectadaedadgenero
)

# Llamar a la función y obtener las variables (para nombres de tablas y conexiones)
config_variables = get_variables(
    params=[
        "db_connection_id",
        "DA_AfectadaEdadGenero",
        "solicitudes",
        "personasafectada",
        "avances",
        "avanceestados",
        "metadatos",
        "tipometadato",
        "tipolugar",
        "areas",
        "canal",
        "tipopeticions"
    ]
)

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
DA_AfectadaEdadGenero_table_name = next(iter(config_variables["DA_AfectadaEdadGenero"].values()))[0]
solicitudes_table_name = next(iter(config_variables["solicitudes"].values()))[0]
afectada_table_name = next(iter(config_variables["personasafectada"].values()))[0]
avances_table_name = next(iter(config_variables["avances"].values()))[0]
avanceestados_table_name = next(iter(config_variables["avanceestados"].values()))[0]
metadatos_table_name = next(iter(config_variables["metadatos"].values()))[0]
tipometadato_table_name = next(iter(config_variables["tipometadato"].values()))[0]
tipolugar_table_name = next(iter(config_variables["tipolugar"].values()))[0]
areas_table_name = next(iter(config_variables["areas"].values()))[0]
canal_table_name = next(iter(config_variables["canal"].values()))[0]
tipopeticions_table_name = next(iter(config_variables["tipopeticions"].values()))[0]
schedule = config_variables["schedule"]


# Definición del DAG
with DAG(
    dag_id="sis-parte-08",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=schedule,
    catchup=False,
    default_args=default_args,
    tags=["sis", "datos"],
    doc_md="""### DAG: sis-parte-08
    Este DAG realiza un proceso de integración de datos en la tabla `DA_AfectadaEdadGenero`. Los pasos incluyen:
    
    1. Truncar la tabla de destino `DA_AfectadaEdadGenero` para limpiar datos previos.
    2. Insertar datos desde varias tablas relacionadas (`solicitudes`, `afectada`, etc.) en `DA_AfectadaEdadGenero, ademas de la informacion adicional provenientes de otras tablas

    Este flujo de trabajo está diseñado para consolidar datos dispersos en varias tablas y almacenarlos en una única tabla de destino para análisis o procesamiento posterior.
    """
) as dag:

    truncate_db_table_task = SQLExecuteQueryOperator(
        task_id=f'truncate_db_table_task',
        conn_id=db_connection_id,
        sql=truncate_sql,
        params={
            'table_name': DA_AfectadaEdadGenero_table_name,
        },
        doc_md="""### Tarea: Truncar la tabla de destino
        Esta tarea elimina todos los datos en la tabla `DA_AfectadaEdadGenero` antes de realizar las inserciones,
        para asegurar que los datos se actualicen completamente en cada ejecución.
        """
    )
    
    insert_db_task = SQLExecuteQueryOperator(
        task_id="insert_db_task",
        conn_id=db_connection_id,
        sql=insert_sql_da_afectadaedadgenero,
        params={
            'DA_AfectadaEdadGenero_table': DA_AfectadaEdadGenero_table_name,
            'solicitudes_table': solicitudes_table_name,
            'afectada_table': afectada_table_name,
            'avances_table': avances_table_name,
            'avanceestados_table': avanceestados_table_name,
            'metadatos_table': metadatos_table_name,
            'tipometadato_table': tipometadato_table_name,
            'tipolugar_table': tipolugar_table_name,
            'areas_table':areas_table_name,
            'canal_table':canal_table_name,
            'tipopeticions_table':tipopeticions_table_name
            
            
        },
        autocommit=True,
        execution_timeout=timedelta(minutes=15),
        doc_md="""### Tarea: Insertar datos en la tabla de destino
        Esta tarea inserta datos en `DA_AfectadaEdadGenero` desde varias tablas (`solicitudes`, `afectada`, `avances`, etc.),
        uniendo información de múltiples tablas para crear un dataset consolidado.
        """
    )
    
    truncate_db_table_task >> insert_db_task
