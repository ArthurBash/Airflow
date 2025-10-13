import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import logging

from app.common.minio_utils import get_minio_client
from app.common.mongo_utils import get_mongo_db

from airflow.exceptions import AirflowException
from bson import ObjectId

  
def clean_objectids(data):
# Limpieza de los objetos_IDs en STR
  """
  Convierte TODOS los ObjectId a string de forma recursiva.
  Funciona con cualquier estructura: listas, diccionarios, anidados, etc.
  """
  if isinstance(data, ObjectId):
    return str(data)
  elif isinstance(data, dict):
    return {key: clean_objectids(value) for key, value in data.items()}
  elif isinstance(data, list):
    return [clean_objectids(item) for item in data]
  else:
    return data

def clean_nat(df):
    import numpy as np
    
    # Detectar columnas datetime automáticamente
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    
    # NUEVO: Detectar columnas que DEBERÍAN ser datetime pero están como object
    potential_datetime_cols = [col for col in df.columns 
                               if 'tedat' in col.lower()]
    
    # Procesar columnas datetime detectadas
    for col in datetime_cols:
        df[col] = df[col].where(df[col].notna(), None)
    
    # NUEVO: Forzar conversión de columnas sospechosas
    for col in potential_datetime_cols:
        if col not in datetime_cols and col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].where(df[col].notna(), None)
            except:
                pass  # Si falla, dejar como está
    
    return df


def store_in_minio_parquet(
    data: list,
    bucket_name: str,
    object_name: str,
    minio_id: str,
    chunk_size: int = 50000
) -> None:
    """
    Almacena registros en formato Parquet en Minio, con soporte para chunking.
    
    Args:
        data (list): Registros a guardar (desde MongoDB)
        bucket_name (str): Nombre del bucket de Minio
        object_name (str): Ruta del objeto (sin extensión, se añade .parquet)
        minio_id (str): ID de la conexión Airflow para MinIO
        chunk_size (int): Número de registros por chunk
    """
    hook = get_minio_client(minio_id)
    
    try:
        total_records = len(data)
        
        if total_records == 0:
            logging.warning("No hay datos para guardar")
            return
        
        # Si los datos son pequeños, guardar en un solo archivo
        if total_records <= chunk_size:
            _save_single_parquet_chunk(hook, data, bucket_name, object_name)
        else:
            # Para datos grandes, dividir en chunks
            _save_multiple_parquet_chunks(hook, data, bucket_name, object_name, chunk_size)
            
    except Exception as e:
        logging.error(f"Error inesperado en {store_in_minio_parquet.__name__}: {e}")
        raise AirflowException(f"Error al guardar datos en MinIO: {e}")


def _save_single_parquet_chunk(hook, data, bucket_name, object_name):
    """Guarda un solo chunk de datos"""

    data = clean_objectids(data)
    df = pd.DataFrame(data)
    
    df = clean_nat(df)
    
    
    # Convertir a parquet en memoria
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy',
                   coerce_timestamps="us",              
                  allow_truncated_timestamps=True)
    buffer.seek(0)
    
    # Subir a MinIO
    final_object_name = f"{object_name}.parquet"
    hook.load_bytes(
        bytes_data=buffer.getvalue(),
        key=final_object_name,
        bucket_name=bucket_name,
        replace=True
    )
    
    logging.info(f"Guardados {len(data)} registros en '{final_object_name}' del bucket '{bucket_name}'")


def _save_multiple_parquet_chunks(hook, data, bucket_name, object_name, chunk_size):
    """Guarda múltiples chunks de datos"""
    total_chunks = 0

    for i in range(0, len(data), chunk_size):
        chunk_data = data[i:i + chunk_size]
        chunk_data = clean_objectids(chunk_data)
        chunk_number = (i // chunk_size) + 1
        
        df = pd.DataFrame(chunk_data)

        df = clean_nat(df)
      
        # Crear buffer para este chunk
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy',
                      coerce_timestamps="us",             
                      allow_truncated_timestamps=True)
        buffer.seek(0)
        
        # Nombre con número de chunk
        chunk_object_name = f"{object_name}_chunk_{chunk_number:04d}.parquet"
        
        hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=chunk_object_name,
            bucket_name=bucket_name,
            replace=True
        )
        
        total_chunks += 1
        logging.info(f"Chunk {chunk_number}: {len(chunk_data)} registros guardados en '{chunk_object_name}'")
    
    logging.info(f"Proceso completado: {len(data)} registros totales en {total_chunks} chunks")


# FUNCIÓN AUXILIAR MEJORADA
def clean_datetime_columns(df, datetime_columns=None):
    """
    Limpia columnas de datetime, convirtiendo NaT a None de forma segura
    """
    if datetime_columns is None:
        datetime_columns = ["createdAt", "updatedAt"]
    
    for col in datetime_columns:
        if col in df.columns:
            # Convertir a datetime
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # Convertir NaT a None para compatibilidad con PostgreSQL
            df[col] = df[col].where(df[col].notna(), None)
    
    return df


def download_from_minio_parquet_chunked(minio_id, bucket_name, object_pattern, read_chunk_size=10000):
    """
    Generator que lee archivos Parquet por chunks desde MinIO
    
    Args:
        object_pattern: patrón como "collection_name" (buscará collection_name*.parquet)
        read_chunk_size: registros por chunk al leer
    """
    hook = get_minio_client(minio_id)
    
    try:
        # Listar todos los archivos parquet que coincidan con el patrón
        objects = hook.list_objects(bucket_name, prefix=object_pattern)
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        
        if not parquet_files:
            logging.warning(f"No se encontraron archivos parquet con el patrón '{object_pattern}'")
            return
        
        logging.info(f"Encontrados {len(parquet_files)} archivos parquet para procesar")
        
        # Procesar cada archivo
        for file_name in sorted(parquet_files):
            logging.info(f"Procesando archivo: {file_name}")
            
            # Descargar archivo a memoria
            file_obj = hook.download_file(key=file_name, bucket_name=bucket_name)
            
            # Leer parquet con pandas y hacer chunking
            df = pd.read_parquet(file_obj, engine='pyarrow')
            
            # Convertir a chunks
            for i in range(0, len(df), read_chunk_size):
                chunk_df = df.iloc[i:i + read_chunk_size]
                # Convertir de vuelta a lista de diccionarios (formato original)
                chunk_documents = chunk_df.to_dict('records')
                yield chunk_documents
                
    except Exception as e:
        logging.error(f'Error al hacer streaming de Parquet desde MinIO: {e}')
        raise



def extract_mongo_to_minio_parquet(
    connection_id: str,
    database_name: str,
    collection: str,
    bucket_name: str,
    object_name: str,
    minio_id: str,
    extra_column,
    filter: dict = None,
    projection: dict = None,
    chunk_size: int = 50000 
):
    """
    Extrae de MongoDB y guarda directamente en Parquet por chunks
    """
    try:
        db = get_mongo_db(connection_id, database_name)
        
        filter = filter or {}
        projection = projection or {}
        
        cursor = db[collection].find(filter, projection)
        
        # Procesar en chunks para evitar cargar todo en memoria
        chunk_data = []
        chunk_number = 0
        
        for document in cursor:
            if extra_column:
              extra = extra_column(document)
              document.update(extra)
              
            chunk_data.append(document)
            
            # Cuando llegamos al tamaño del chunk, guardamos
            if len(chunk_data) >= chunk_size:
                chunk_number += 1
                chunk_object_name = f"{object_name}_part_{chunk_number:04d}"
                
                store_in_minio_parquet(
                    data=chunk_data,
                    bucket_name=bucket_name,
                    object_name=chunk_object_name,
                    minio_id=minio_id,
                    chunk_size=chunk_size  # No hacer sub-chunking, ya estamos chunkeando
                )
                
                chunk_data = []  # Limpiar buffer
        
        # Guardar el último chunk si no está vacío
        if chunk_data:
            chunk_number += 1
            chunk_object_name = f"{object_name}_part_{chunk_number:04d}"
            store_in_minio_parquet(
                data=chunk_data,
                bucket_name=bucket_name,
                object_name=chunk_object_name,
                minio_id=minio_id,
                chunk_size=len(chunk_data)
            )
        
        logging.info(f"Extracción completada: {chunk_number} chunks guardados")
        
    except Exception as e:
        logging.error(f"Error en extracción MongoDB a Parquet: {e}")
        raise


def delete_prefix_objects(bucket_name: str, prefix: str, aws_conn_id: str = "aws_s3"):
    hook = get_minio_client(aws_conn_id)

    logging.info(f"el bucket {bucket_name}  prefix {prefix}")
    # Listar keys bajo el prefijo
    keys = hook.list_keys(bucket_name, prefix) or []

    if not keys:
        logging.info(f"No se encontraron objetos en {prefix} para eliminar.")
        return 0

    # Borrar en lote
    hook.delete_objects(bucket_name, keys)

    logging.info(f"Se eliminaron {len(keys)} objetos en {prefix}.")
    return len(keys)
