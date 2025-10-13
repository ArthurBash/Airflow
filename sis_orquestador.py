# Imports de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pendulum

# Importaciones locales
from app.sis.common import (
    poke_interval,
    DISCORD_WEBHOOK_URL
)

# Otros imports
from datetime import datetime, timezone, timedelta
import requests

def notify_failure(context):
    """
    Envía una notificación a Discord en caso de que una tarea falle.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    
    message = (f"❌ El DAG {dag_id} ha fallado en la tarea {task_id}.\n"
               f"Fecha de ejecución: {execution_date}\n"
               f"Logs: {log_url}")
    
    send_discord_notification(message)

def send_discord_notification(message: str):
    """
    Función para enviar una notificación a Discord usando un webhook.
    """
    data = {
        "content": message,  # El contenido del mensaje que se enviará
        "username": "Airflow Bot",  # Nombre de quien envía el mensaje
    }
    response = requests.post(DISCORD_WEBHOOK_URL, json=data)
    if response.status_code == 204:
        print("Mensaje enviado a Discord")
    else:
        print(f"Error al enviar mensaje a Discord: {response.status_code}")

def notify_execution_duration(**context):
    """
    Envía una notificación a Discord indicando la duración de la ejecución del DAG.
    """
    dag_run = context.get('dag_run')
    start_time = dag_run.start_date
    end_time = dag_run.end_date or datetime.now(timezone.utc)
    duration = end_time - start_time
    
    # Obtener la duración en minutos y segundos
    duration_in_minutes, seconds = divmod(duration.total_seconds(), 60)

    message = f"El DAG {dag_run.dag_id} ha finalizado. La ejecución duró {int(duration_in_minutes)} minutos y {int(seconds)} segundos."
    
    send_discord_notification(message)

# Definición del DAG Orquestador
with DAG(
    dag_id="sis_orquestador",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule="0 22 * * 1-5",
    catchup=False,
    default_args={
        "owner": "Datos MJUS",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "concurrency": 1,
        "tags": ["sis", "datos"],
    },
    tags=["orquestador", "sis"],
    doc_md="""### Orquestador de Procesos SIS
    Este DAG orquesta la ejecución de varios DAGs relacionados con el procesamiento del sistema SIS.
    
    Las tareas de este DAG incluyen el disparo de otros DAGs de procesamiento, como `sis-parte-02` al `sis-parte-11`, y la notificación de fallos o duración de ejecución.
    """,
) as dag:
    
    trigger_dag_get = TriggerDagRunOperator(
        task_id="trigger_dag_get",
        trigger_dag_id="sis-get",
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(minutes=5),
        do_xcom_push=False, 
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-get`
        Esta tarea dispara el DAG `sis-get`, el cual es responsable de obtener datos para los siguientes pasos de procesamiento.
        """
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_02",
        trigger_dag_id="sis-parte-02",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-02`
        Esta tarea dispara el DAG `sis-parte-02`, que se encarga de procesar datos relacionados con el sistema SIS.
        """
    )

    trigger_dag_3 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_03",
        trigger_dag_id="sis-parte-03",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-03`
        Esta tarea dispara el DAG `sis-parte-03`, que maneja otro conjunto de datos dentro del flujo del sistema SIS.
        """
    )

    trigger_dag_4 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_04",
        trigger_dag_id="sis-parte-04",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-04`
        Esta tarea dispara el DAG `sis-parte-04`, que procesa un segmento adicional de datos dentro del flujo del sistema SIS.
        """
    )

    trigger_dag_5 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_05",
        trigger_dag_id="sis-parte-05",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-05`
        Esta tarea dispara el DAG `sis-parte-05`, que sigue procesando datos dentro del flujo del sistema SIS.
        """
    )

    trigger_dag_6 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_06",
        trigger_dag_id="sis-parte-06",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-06`
        Esta tarea dispara el DAG `sis-parte-06`, el último de los DAGs iniciales en el flujo del sistema SIS.
        """
    )

    # 2. Dependencias secuenciales (sis-parte-07 al 11)
    trigger_dag_7 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_07",
        trigger_dag_id="sis-parte-07",
        wait_for_completion=True,
        execution_timeout=timedelta(minutes=40),
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-07`
        Esta tarea dispara el DAG `sis-parte-07`, el cual depende de los DAGs previos y sigue el flujo de procesamiento del sistema SIS.
        """
    )

    trigger_dag_8 = TriggerDagRunOperator(
      task_id="trigger_dag_sis_parte_08",
      trigger_dag_id="sis-parte-08",
      wait_for_completion=True,
      do_xcom_push=False, 
      poke_interval=poke_interval,
      on_failure_callback=notify_failure,
      doc_md="""### Disparar el DAG `sis-parte-08`
      Esta tarea dispara el DAG `sis-parte-08`, que continúa con la ejecución del flujo del sistema SIS.
        """
    )

    trigger_dag_9 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_09",
        trigger_dag_id="sis-parte-09",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-09`
        Esta tarea dispara el DAG `sis-parte-09`, para continuar con el procesamiento de los datos en el flujo del sistema SIS.
        """
    )

    trigger_dag_10 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_10",
        trigger_dag_id="sis-parte-10",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-10`
        Esta tarea dispara el DAG `sis-parte-10`, que sigue procesando los datos dentro del flujo de trabajo.
        """
    )

    trigger_dag_11 = TriggerDagRunOperator(
        task_id="trigger_dag_sis_parte_11",
        trigger_dag_id="sis-parte-11",
        wait_for_completion=True,
        do_xcom_push=False, 
        poke_interval=poke_interval,
        on_failure_callback=notify_failure,
        doc_md="""### Disparar el DAG `sis-parte-11`
        Esta tarea dispara el DAG `sis-parte-11`, el último en la secuencia de procesamiento de los datos del sistema SIS.
        """
    )
    
    send_final_notification = PythonOperator(
        task_id="send_final_notification",
        python_callable=notify_execution_duration,
        do_xcom_push=False, 
        doc_md="""### Notificación de Finalización
        Esta tarea envía una notificación a Discord informando sobre la duración total de la ejecución del DAG.
        """
    )
  
    trigger_dag_get >> [trigger_dag_2, trigger_dag_3, trigger_dag_4, trigger_dag_5, trigger_dag_6] >> trigger_dag_7  >> trigger_dag_8 >> trigger_dag_9 >> trigger_dag_10 >> trigger_dag_11  >> send_final_notification
