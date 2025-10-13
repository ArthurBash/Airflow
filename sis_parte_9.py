# Importaciones de Airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pendulum

# Importaciones locales
from app.sis.common import get_variables
from app.sql_queries import (
    actualizar_creador_en_solicitudes_query,
    actualizar_tipo_lugar_en_solicitudes_query,
    actualizar_area_parent_en_solicitudes_query,
    actualizar_canal_en_solicitudes_query,
    actualizar_tipo_subtipo_en_solicitudes_query,
    actualizar_estado_en_solicitudes_query,
    actualizar_usuario_areaCarga_en_solicitudes_query,
    actualizar_complejo_en_solicitudes_query,
    actualizar_entidades_juridicas_query,
    actualizar_datos_sin_joins_en_solicitudes_query
)
from datetime import timedelta

# Llamar a la función y obtener las variables
config_variables = get_variables(
    params=[
        "db_connection_id",
        "solicitudes",
        "avances",
        "avanceestados",
        "areas",
        "canal",
        "tipopeticions",
        "tipolugar",
        "usuarios",
        "unidades",
        "entidadjuridicas",
    ]
)

# Acceder a las variables
default_args = config_variables["default_args"]
db_connection_id = config_variables["db_connection_id"]
solicitudes_table_name = next(iter(config_variables["solicitudes"].values()))[0]
avances_table_name = next(iter(config_variables["avances"].values()))[0]
avanceestados_table_name = next(iter(config_variables["avanceestados"].values()))[0]
areas_table_name = next(iter(config_variables["areas"].values()))[0]
canal_table_name = next(iter(config_variables["canal"].values()))[0]
tipopeticions_table_name = next(iter(config_variables["tipopeticions"].values()))[0]
tipolugar_table_name = next(iter(config_variables["tipolugar"].values()))[0]
usuarios_table_name = next(iter(config_variables["usuarios"].values()))[0]
unidades_table_name = next(iter(config_variables["unidades"].values()))[0]
entidadjuridicas_table_name = next(iter(config_variables["entidadjuridicas"].values()))[0]
schedule = config_variables["schedule"]

with DAG(
    dag_id='sis-parte-09',
    default_args=default_args,
    description='Descripción del DAG',
    schedule=schedule,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    tags = ["sis", "datos"],
    doc_md="""
    ### sis-parte-09 DAG
    Este DAG realiza varias actualizaciones en la tabla `solicitudes`, utilizando los datos de diversas tablas relacionadas 
    como `avances`, `areas`, `canal`, `tipopeticions`, `tipolugar`, `usuarios`, `unidades` y `entidadjuridicas`.
    Cada tarea en este flujo se encarga de actualizar campos específicos en `solicitudes` según la información de las tablas relacionadas.
    """
) as dag:
  
    update_creador_en_solicitudes = SQLExecuteQueryOperator(
        task_id='update_creador_en_solicitudes',
        conn_id=db_connection_id,
        sql=actualizar_creador_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_avances': avances_table_name,
            'campo_avance_solicitud_id':"solicitud_id",
            'campo_solicitud_id': "_id",
            'campo_usuario_id': "usuario_id",
        },
        execution_timeout=timedelta(minutes=20),
        doc_md="""
        #### update_creador_en_solicitudes
        Actualiza el creador en `solicitudes` basándose en la relación con `avances`.
        """
    )
    
    update_solicitudes_tipo_lugar = SQLExecuteQueryOperator(
        task_id='update_solicitudes_tipo_lugar',
        conn_id=db_connection_id,
        sql=actualizar_tipo_lugar_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_tipolugar': tipolugar_table_name,
            'campo_solicitud_id': "tipolugar_id",
            'campo_nombre_tipo_lugar': "tipolugar",
        },
        doc_md="""
        #### update_solicitudes_tipo_lugar
        Actualiza el campo `tipolugar` en `solicitudes` utilizando información de `tipolugar`.
        """
    )

    update_solicitudes_area_parent = SQLExecuteQueryOperator(
        task_id='update_solicitudes_area_parent',
        conn_id=db_connection_id,
        sql=actualizar_area_parent_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_areas': areas_table_name,
            'campo_solicitud_id': "area_id",
            'campo_nombre_area': "area",
        },
        doc_md="""
        #### update_solicitudes_area_parent
        Actualiza el campo `area` en `solicitudes` en base a los datos de `areas`.
        """
    )

    update_solicitudes_canal = SQLExecuteQueryOperator(
        task_id='update_solicitudes_canal',
        conn_id=db_connection_id,
        sql=actualizar_canal_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_canal': canal_table_name,
            'campo_solicitud_id': "canal_id",
            'campo_nombre_canal': "canal",
        },
        doc_md="""
        #### update_solicitudes_canal
        Actualiza el campo `canal` en `solicitudes` a partir de los datos en `canal`.
        """
    )

    update_solicitudes_tipo_subtipo = SQLExecuteQueryOperator(
        task_id='update_solicitudes_tipo_subtipo',
        conn_id=db_connection_id,
        sql=actualizar_tipo_subtipo_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_tipopeticions': tipopeticions_table_name,
            'campo_subtipo': "subtipo_id",
            'campo_tipo': "tipo_id",
            'campo_subtipo_nombre': "subtipo",
            'campo_tipo_nombre': "tipo",
        },
        doc_md="""
        #### update_solicitudes_tipo_subtipo
        Actualiza los campos `subtipo` y `tipo` en `solicitudes` utilizando datos de `tipopeticions`.
        """
    )

    update_solicitudes_estado = SQLExecuteQueryOperator(
        task_id='update_solicitudes_estado',
        conn_id=db_connection_id,
        sql=actualizar_estado_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_avanceestados': avanceestados_table_name,
            'campo_estado_id': "estado_id",
            'campo_estado_nombre': "estado",
        },
        doc_md="""
        #### update_solicitudes_estado
        Actualiza el campo `estado` en `solicitudes` usando información de `avanceestados`.
        """
    )

    update_solicitudes_usuario_areaCarga = SQLExecuteQueryOperator(
        task_id='update_solicitudes_usuario_areaCarga',
        conn_id=db_connection_id,
        sql=actualizar_usuario_areaCarga_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_usuarios': usuarios_table_name,
            'table_areas': areas_table_name,
            'campo_solicitudes_usuario_id': "usuario_id",
            'campo_usuarios_id': "_id",
            'campo_usuarios_area_id': "area",
            'campo_solicitudes_usuario_carga': "usuario_carga",
            'campo_solicitudes_area_carga': "area_carga",
        },
        doc_md="""
        #### update_solicitudes_usuario_areaCarga
        Actualiza los campos `usuario_carga` y `area_carga` en `solicitudes` utilizando los datos de `usuarios` y `areas`.
        Esta tarea establece qué usuario y área fueron responsables de la carga en cada solicitud.
        """
    )

    update_solicitudes_agrega_complejo = SQLExecuteQueryOperator(
        task_id='update_solicitudes_agrega_complejo',
        conn_id=db_connection_id,
        sql=actualizar_complejo_en_solicitudes_query,
        params={
            'table_solicitudes': solicitudes_table_name,
            'table_unidades': unidades_table_name,
            'campo_solicitudes_lugar': "lugar",
            'campo_unidades_nombre': "nombre",
            'campo_solicitudes_complejo': "complejo",
            'campo_unidades_complejo': "complejo",
        },
        doc_md="""
        #### update_solicitudes_agrega_complejo
        Actualiza el campo `complejo` en `solicitudes` utilizando los datos de `unidades`. 
        Este campo indica el complejo al cual pertenece la unidad asociada a cada solicitud.
        """
    )

    update_solicitudes_entidades_juridicas = SQLExecuteQueryOperator(
        task_id='update_solicitudes_entidades_juridicas',
        conn_id=db_connection_id,
        sql=actualizar_entidades_juridicas_query,
        params={
            'table_entidades': entidadjuridicas_table_name,
            'campo_entidades_nombre': "nombre",
            'campo_entidades_departamento_judicial': "departamento_judicial",
        },
        doc_md="""
        #### update_solicitudes_entidades_juridicas
        Actualiza la información sobre `entidades_juridicas` en `solicitudes`. 
        Esta tarea usa el nombre y el departamento judicial de las entidades jurídicas para complementar los datos en `solicitudes`.
        """
    )

 #   update_solicitudes_evitar_joins = SQLExecuteQueryOperator(
 #       task_id='update_solicitudes_evitar_joins',
 #       conn_id=db_connection_id,
 #       sql=actualizar_datos_sin_joins_en_solicitudes_query,
 #       params={
 #           'table_solicitudes': solicitudes_table_name,
 #           'table_tipolugar': tipolugar_table_name,
 #           'table_areas': areas_table_name,
 #           'table_canal': canal_table_name,
 #           'table_tipopeticions': tipopeticions_table_name,
 #           'table_usuarios': usuarios_table_name
 #       },
 #       doc_md="""
 #       #### update_solicitudes_evitar_joins
 #       Realiza actualizaciones en `solicitudes` sin el uso de joins directos, optimizando la consulta.
 #       Esta tarea permite actualizar múltiples campos en `solicitudes` (incluyendo campos relacionados con `tipolugar`, `areas`, `canal`, `tipopeticions`, y `usuarios`) sin utilizar joins para mejorar la eficiencia.
 #       """
 #   )

    update_creador_en_solicitudes >> update_solicitudes_tipo_lugar >> update_solicitudes_area_parent >> update_solicitudes_canal >> update_solicitudes_tipo_subtipo >> update_solicitudes_estado >> update_solicitudes_usuario_areaCarga >> update_solicitudes_agrega_complejo >> update_solicitudes_entidades_juridicas
