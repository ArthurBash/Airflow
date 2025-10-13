# Importaciones de Airflow
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Importaciones locales
from app.common.mongo_utils import get_mongo_db
from app.common.minio_utils import download_from_minio, get_minio_client
from app.common.shared_utils import json_converter

# Otras importaciones
import json
import os
import logging
from datetime import timedelta, datetime
from io import BytesIO

poke_interval = 10
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1286697783926980761/6lnsC6sAXZX2Eqxjsl2bSolpMIcq4zv06kgAR1yLkF4UyRVgMfJrhRR8iwWNXHWnWmB-"

def get_db_hook(connection_id):
    """Devuelve el hook adecuado según el tipo de conexión."""
    if "mysql" in connection_id:
        return MySqlHook(mysql_conn_id=connection_id)
    elif "postgres" in connection_id:
        return PostgresHook(postgres_conn_id=connection_id)
    else:
        raise ValueError(f"Tipo de conexión no soportada para {connection_id}")
        
def insert_sql_from_minio_areas(minio_id, bucket_name, collection, columns, table, connection_id, batch_size, extra_columns_func=None):
    try:
        documents = download_from_minio(minio_id, bucket_name, collection)
    except Exception as e:
        logging.error(f'Error al leer datos desde Minio: {e}')
        raise
        
    if not documents:
        logging.warning(f'No hay documentos para insertar en {table}.')
        return
    
    hook = get_db_hook(connection_id)
    try:
        # Ordenar los documentos por dependencias
        ordered_documents = ordenar_por_dependencias(documents)
        
        # Crear los placeholders para la query
        values_placeholders = ', '.join(['%s' for _ in columns])
        insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values_placeholders})"
        
        logging.info(f'Iniciando inserciones en la tabla: {table}')
        
        total_rows = 0
        for i in range(0, len(ordered_documents), batch_size):
            batch = ordered_documents[i:i + batch_size]
            
            rows = process_documents_areas(batch, columns, extra_columns_func)
            logging.warning(f"Batch size: {len(rows)}")  # Agrega este log para ver el tamaño del batch
            
            # Ejecutar la query para cada fila individualmente
            for row in rows:
                hook.run(insert_query, parameters=row)
            
            total_rows += len(rows)
        
        logging.info(f'Se han insertado {total_rows} registros en la tabla {table}.')
            
    except Exception as e:
        logging.error(f'Error al insertar datos en la BD: {e}')
        raise

def insert_sql_from_minio(minio_id, bucket_name, collection, columns, table, connection_id, batch_size, extra_columns_func=None):
    try:
        documents = download_from_minio(minio_id, bucket_name, collection)
    except Exception as e:
        logging.error(f'Error al leer datos desde Minio: {e}')
        raise

    if not documents:
        logging.warning(f'No hay documentos para insertar en {table}.')
        return
    
    hook = get_db_hook(connection_id)
    
    values_placeholders = ', '.join(['%s' for _ in columns])
    if 'mongo_updated_at' in columns:
        condition = f"WHERE {table}.mongo_updated_at <> EXCLUDED.mongo_updated_at"
    else:
        condition = ""

    insert_query = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({values_placeholders})
        ON CONFLICT (_id) DO UPDATE
        SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != '_id'])}
        {condition}
    """

    total_rows = 0
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                rows = process_documents(batch, columns, extra_columns_func)
                cursor.executemany(insert_query, rows)
                total_rows += len(rows)
            conn.commit()

    logging.info(f'Se han insertado {total_rows} registros en la tabla {table}.')

def process_documents(batch, columns, extra_columns_func=None):
    """
    Procesa una lista de documentos JSON y los transforma en una lista de filas para insertar en una tabla Postgres.

    Args:
        batch (list): Lista de documentos JSON.
        columns (list): Lista de nombres de columnas en la tabla Postgres.
        extra_columns_func (function): Función para generar valores adicionales para las columnas (opcional).

    Returns:
        list: Lista de filas, donde cada fila es una lista de valores para insertar en la tabla Postgres.
    """
    rows = []
    for document in batch:
        # Crear un diccionario inicial con los valores de las columnas especificadas
        diccionario = {column: document.get(column, None) for column in columns}

        # # Si hay una función para columnas adicionales, aplicarla
        # if extra_columns_func:
        #     diccionario.update(extra_columns_func(document))

        # Crear una fila con los valores para cada columna
        row = [diccionario.get(column) for column in columns]
        rows.append(row)
    return rows

def process_documents_areas(batch, columns, extra_columns_func=None):
    rows = []
    for doc in batch:
        row = []
        for col in columns:
            # Special handling for 'parent' column
            if col == 'parent':
                # Use None if parent is not present or is None
                row.append(doc.get('parent', None))
            else:
                # For other columns, use get() with a default value
                row.append(doc.get(col, None))
        
        # Agregar columnas extra si se proporciona la función
        if extra_columns_func:
            extra_cols = extra_columns_func(doc)
            row.extend(extra_cols)
        
        rows.append(row)
    
    return rows

def ordenar_por_dependencias(registros):
    # Crear un diccionario para acceso rápido por _id
    registros_dict = {registro["_id"]: registro for registro in registros}

    # Inicializar el orden de inserción vacío y un conjunto de _ids ya ordenados
    orden_insercion = []
    procesados = set()

    # Función recursiva para insertar en orden según las dependencias
    def insertar_en_orden(registro):
        # Extraer el parent_id como string (None si no tiene)
        # parent_id = registro["parent"]["$oid"] if registro["parent"] and isinstance(registro["parent"], dict) else None
        parent_id = registro.get('parent')

        # Si el parent es nulo o ya está procesado, se puede agregar este registro
        if parent_id is None or parent_id in procesados:
            if registro["_id"] not in procesados:  # El primer elemento es el _id
            # if registro["_id"]["$oid"] not in procesados:
                orden_insercion.append(registro)
                # procesados.add(registro["_id"]["$oid"])
                procesados.add(registro["_id"])
        else:
            # Si el parent aún no está procesado, intentar agregar el registro del parent primero
            parent_registro = registros_dict.get(parent_id)
            if parent_registro:
                insertar_en_orden(parent_registro)
            # Después de intentar agregar el parent, agregar el registro actual
            if registro["_id"] not in procesados:
            # if registro["_id"]["$oid"] not in procesados:
                orden_insercion.append(registro)
                # procesados.add(registro["_id"]["$oid"])
                procesados.add(registro["_id"])

    # Procesar cada registro y construir el orden
    for registro in registros:
        insertar_en_orden(registro)
    
    return orden_insercion

def insert_data_areas(hook, insert_query, rows):
    """
    Inserta datos utilizando el hook de base de datos proporcionado.

    Args:
        hook: Hook de base de datos.
        insert_query (str): Consulta de inserción.
        rows (list): Filas a insertar.
    """
    hook.run(insert_query, parameters=rows, autocommit=True)
        
def insert_data(mysql_hook, insert_query, rows):
    """
    Inserta una lista de filas en una tabla Postgres usando un batch.

    Args:
        mysql_hook (MySqlHook): Hook de Airflow para interactuar con Postgres.
        insert_query (str): Consulta SQL para la inserción.
        rows (list): Lista de filas a insertar.
    """
    try:
        conn = mysql_hook.get_conn()
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, rows)
            conn.commit()
        logging.info(f'Inserción exitosa del batch de {len(rows)} registros en la tabla Postgres.')
    except Exception as e:
        logging.error(f'Error al insertar datos en Postgres. Error completo: "{e}"')
        raise

def parse_time(time_str):
    """
    Convierte una cadena de hora en un objeto time, o retorna None si no es válida.

    Args:
        time_str (str): Cadena de hora en formato %H:%M:%S o %H:%M.

    Returns:
        datetime.time or None: Objeto time o None si la cadena no es válida.
    """
    if time_str:
        try:
            return datetime.strptime(time_str.strip(), "%H:%M:%S").time()
        except ValueError:
            try:
                return datetime.strptime(time_str.strip(), "%H:%M").time()
            except ValueError:
                return None
    return None


def parse_date(date_str):
    """
    Convierte una cadena de fecha en un objeto datetime, o retorna None si no es válida.

    Args:
        date_str (str): Cadena de fecha en formato ISO.

    Returns:
        datetime.datetime or None: Objeto datetime o None si la cadena no es válida.
    """
    
    if date_str is None or date_str == "NaT" or date_str == "NaN":
        return None

    try:
        if isinstance(date_str, datetime):
            return date_str
        else:
            return datetime.fromisoformat(str(date_str))
    except Exception as e:
        logging.warning(f"No se pudo convertir '{date_str}' a datetime: {e}")
        return None  # CRÍTICO: Siempre devolver None en caso de error


def load_config():
    """
    Carga el archivo de configuración y devuelve su contenido como un diccionario.

    Raises:
        FileNotFoundError: Si el archivo de configuración no se encuentra.

    Returns:
        dict: Contenido del archivo de configuración.
    """
    # Obtener el directorio del archivo DAG
    dag_directory = os.path.dirname(__file__)

    # Ruta al archivo de configuración
    CONFIG_FILE = os.path.join(dag_directory, 'config.json')

    # Verificar si el archivo existe
    if os.path.exists(CONFIG_FILE):
        # Cargar las variables desde el archivo de configuración
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        return config
    else:
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {CONFIG_FILE}")
    
def get_variables(params=None):
    """
    Obtiene variables de configuración para una o más tablas y/o atributos adicionales.

    Args:
        params (list, optional): Una lista de tablas y/o atributos adicionales a incluir.

    Returns:
        dict: Variables de configuración solicitadas.
    """
    config = load_config()

    variables = {}

    if params:
        for param in params:
            if param in config:
                variables[param] = config[param]
    
    variables["default_args"] = {
        "owner": "Datos MJUS",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "concurrency": 1,
        'sql_conn_timeout': 300,
    }
    variables["schedule"] = None
    variables["batch_size"] = 50000

    return variables

def get_mongo_collection_minio(collection, connection_id, database_name, minio_id, bucket_name):
    """
    Obtiene una colección de MongoDB y la guarda en un archivo en Minio.

    Args:
        collection (str): El nombre de la colección en MongoDB.
        connection_id (str): El ID de la conexión de Minio en Airflow.
        database_name (str): El nombre de la base de datos en MongoDB.
        bucket_name (str): El nombre del bucket en Minio.

    Returns:
        None
    """

    # Inicializa el hook de Minio (usando S3Hook como base)
    s3_hook = get_minio_client(minio_id)
    
    # Obtiene la conexión a la base de datos de MongoDB
    db = get_mongo_db(connection_id, database_name)
    
    # Obtiene todos los documentos de la colección
    cursor = db[collection].find()
    documents = list(cursor)
    logging.info(f'Se recuperaron {len(documents)} registros de MongoDB de la colección {collection}')

    # Crea un flujo de datos binarios
    file_obj = BytesIO()

    try:
        # Convertir toda la lista de documentos a un solo objeto JSON
        json_data = json.dumps(documents, default=json_converter, ensure_ascii=False).encode('utf-8')
        
        # Escribe el JSON en el flujo binario
        file_obj.write(json_data)
        
        # Posiciona el puntero del archivo al inicio
        file_obj.seek(0)
        
        # Guarda los datos en Minio
        s3_hook.load_file_obj(
            file_obj,
            key=collection,
            bucket_name=bucket_name,
            replace=True
        )
        logging.info(f"Se guardaron {len(documents)} registros en Minio en el objeto '{collection}' del bucket '{bucket_name}'.")

    except Exception as e:
        logging.error(f'Error al procesar datos para Minio. Error completo: "{e}"')
        raise

def insert_metadatos_from_minio2(minio_id, bucket_name, collection, columns, table, connection_id, batch_size):
    try:
        # Levantamos los datos de Minio
        documents = download_from_minio(minio_id, bucket_name, collection)
    except Exception as e:
        logging.error(f'Error al leer datos desde Minio: {e}')
        raise

    if not documents:
        logging.warning(f'No hay documentos para insertar en {table}.')
        return
    
    # Procesamos los metadatos como venías haciendo
    metadatos = []
    for item in documents:
        if 'metadatos' in item:
            for metadato in item['metadatos']:
                nuevo_metadato = {
                    'avance_id': item['_id'],
                    'tipometadato_id': metadato.get('tipo'),
                    'detalle': metadato.get('detalle'),
                    'fecha': metadato.get('fecha', None)[:10] if metadato.get('fecha') else None
                }
                metadatos.append({'json': nuevo_metadato})

    # Si no hay metadatos, salimos
    if not metadatos:
        logging.warning(f'No se encontraron metadatos para insertar.')
        return

    # Insertamos en la base de datos
    hook = get_db_hook(connection_id)
    
    values_placeholders = ', '.join(['%s' for _ in columns])
    insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values_placeholders})"
    
    logging.info(f'Iniciando inserciones en la tabla: {table}')
    
    total_rows = 0
    for i in range(0, len(metadatos), batch_size):
      #  batch_size=10
        batch = metadatos[i:i + batch_size]
        
        rows = [[metadato['json'].get(col) for col in columns] for metadato in batch]
        
        
        insert_data(hook, insert_query, rows)
        
        total_rows += len(rows)
        
    
    logging.info(f'Se han insertado {total_rows} registros en la tabla {table}.')


def insert_metadatos_from_minio(minio_id, bucket_name, collection, columns, table, connection_id, batch_size, reference_key):
    try:
        documents = download_from_minio(minio_id, bucket_name, collection)
    except Exception as e:
        logging.error(f'Error al leer datos desde Minio: {e}')
        raise

    if not documents:
        logging.warning(f'No hay documentos para insertar en {table}.')
        return
    
    metadatos = []
    for item in documents:
        if 'metadatos' in item:
            for metadato in item['metadatos']:
                metadatos.append({
                    'json': {
                        '_id': metadato['_id'],
                        reference_key: item['_id'],
                        'tipometadato_id': metadato['tipo'],
                        'detalle': metadato['detalle'],
                        'fecha': metadato.get('fecha', None)[:10] if metadato.get('fecha') else None
                    }
                })

    if not metadatos:
        logging.warning(f'No se encontraron metadatos para insertar en {table}.')
        return

    hook = get_db_hook(connection_id)
    
    values_placeholders = ', '.join(['%s' for _ in columns])
  
    insert_query = f"""
    INSERT INTO {table} ({', '.join(columns)}) 
    VALUES ({values_placeholders})
    ON CONFLICT (id) 
    DO UPDATE SET 
        tipometadato_id = EXCLUDED.tipometadato_id,
        detalle = EXCLUDED.detalle,
        fecha = EXCLUDED.fecha;
    """

    total_rows = 0
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(metadatos), batch_size):
                batch = metadatos[i:i + batch_size]
                rows = [[metadato['json'].get(col) for col in columns] for metadato in batch]
                cursor.executemany(insert_query, rows)
                total_rows += len(rows)
            conn.commit()

    logging.info(f'Se han insertado {total_rows} registros en la tabla {table}.')
