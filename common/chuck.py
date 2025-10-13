import io
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.common.minio_utils import get_minio_client
from app.sis.common import get_db_hook

from app.sis.common import process_documents
import tempfile

def _clean_nat_values(documents):
    cleaned = []
    for doc in documents:
        cleaned_doc = {}
        for key, value in doc.items():
            # Manejar TODOS los tipos de NaT/NaN posibles
            if pd.isna(value):  # Esto captura NaT, NaN, None, pd.NA
                cleaned_doc[key] = None
            elif value == "NaT" or value == "NaN":  # String literals
                cleaned_doc[key] = None
            else:
                cleaned_doc[key] = value
        cleaned.append(cleaned_doc)
    return cleaned

def restore_parquet_to_postgres_optimized(
    minio_id: str,
    bucket_name: str,
    path_pattern: str,  # ej: "avances" o "avances/2024/01"
    table: str,
    columns: list,
    connection_id: str,
    read_chunk_size: int = 50000,  # Chunks más grandes para mejor rendimiento
    batch_size: int = 50000  # Lotes de inserción
):
    """
    Lee todos los archivos .parquet de un path en MinIO y los inserta en PostgreSQL
    de forma optimizada con streaming y manejo de memoria eficiente.
    """
    hook_minio = get_minio_client(minio_id)
    hook_postgres = get_db_hook(connection_id)

    # Preparar query de inserción
    values_placeholders = ', '.join(['%s' for _ in columns])
    condition = (
        "WHERE {}.mongo_updated_at <> EXCLUDED.mongo_updated_at".format(table)
        if 'mongo_updated_at' in columns
        else ""
    )

    insert_query = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({values_placeholders})
        ON CONFLICT (_id) DO UPDATE
        SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != '_id'])}
        {condition}
    """

    try:
        # 1. DESCUBRIR ARCHIVOS PARQUET
        logging.info(f"Buscando archivos parquet en: {path_pattern}")
        parquet_files = (
            hook_minio.list_keys(bucket_name=bucket_name, prefix=path_pattern) or []
        )
        parquet_files = [key for key in parquet_files if key.endswith(".parquet")]

        if not parquet_files:
            logging.warning(f"No se encontraron archivos parquet en: {path_pattern}")
            return 0

        # Ordenar archivos para procesamiento predecible
        parquet_files.sort()
        logging.info(f"Encontrados {len(parquet_files)} archivos parquet")

        total_processed = 0
        file_count = 0

        # 2. PROCESAR CADA ARCHIVO DE FORMA STREAMING
        for file_name in parquet_files:
            file_count += 1
            logging.info(
                f"Procesando archivo {file_count}/{len(parquet_files)}: {file_name}"
            )

            file_processed = 0

            try:
                # 3. STREAMING POR CHUNKS DESDE CADA ARCHIVO
                for chunk_documents in _stream_parquet_file_chunked(
                    hook_minio, bucket_name, file_name, read_chunk_size
                ):
                    if not chunk_documents:
                        continue

                    # 4. INSERCIÓN EN LOTES CON TRANSACCIONES GRANULARES
                    chunk_inserted = _insert_chunk_to_postgres(
                        hook_postgres,
                        chunk_documents,
                        insert_query,
                        columns,
                        batch_size
                    )

                    file_processed += chunk_inserted
                    total_processed += chunk_inserted

                    logging.info(f"  Chunk procesado: {chunk_inserted} registros")

            except Exception as e:
                logging.error(f"Error procesando archivo {file_name}: {e}")
                # Continuar con el siguiente archivo, no fallar completamente
                continue

            logging.info(f"Archivo completado: {file_processed} registros")

        logging.info(
            f"Proceso completado: {total_processed} registros totales de {len(parquet_files)} archivos"
        )
        return total_processed

    except Exception as e:
        logging.error(f"Error en proceso de restauración: {e}")
        raise





def _stream_parquet_file_chunked(hook_minio, bucket_name, file_name, chunk_size):
    tmp = None
    try:
        tmp = tempfile.SpooledTemporaryFile(
            max_size=10 * 1024 * 1024
        )  # 10MB en RAM antes de ir a disco
        client = hook_minio.get_conn()

        # Usar posiciónales evita problemas con firmas que no aceptan keyword args
        client.download_fileobj(bucket_name, file_name, tmp)

        tmp.seek(0)
        parquet_file = pq.ParquetFile(tmp)
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            if batch.num_rows:
                documentos = batch.to_pandas().to_dict("records")
                documentos = _clean_nat_values(documentos)
                yield documentos
    except Exception as e:
        logging.error(f"Error leyendo archivo parquet {file_name}: {e}")
        raise

    finally:
        if tmp:
            tmp.close()


def _insert_chunk_to_postgres(
    hook_postgres,
    chunk_documents,
    insert_query,
    columns,
    batch_size
):
    """
    Inserta un chunk de documentos en PostgreSQL con transacciones granulares
    """
    total_inserted = 0

    # Procesar el chunk en lotes más pequeños para optimizar transacciones
    with hook_postgres.get_conn() as conn:
        try:
            with conn.cursor() as cursor:
                for i in range(0, len(chunk_documents), batch_size):
                    batch = chunk_documents[i : i + batch_size]

                    # Procesar documentos (aplicar transformaciones)
                    rows = process_documents(batch, columns, None)
                    

                    # Insertar lote
                    cursor.executemany(insert_query, rows)
                    total_inserted += len(rows)

                # Commit por chunk (transacciones medianas, no muy grandes ni muy pequeñas)
                conn.commit()
                

        except Exception as e:
            conn.rollback()
            logging.error(f"Error insertando chunk: {e}")
            raise

    return total_inserted


def insert_metadatos_from_parquet_streaming(
    minio_id, 
    bucket_name, 
    path_pattern,  # ej: "metadatos/2024/"
    columns, 
    table, 
    connection_id, 
    batch_size, 
    reference_key,
    read_chunk_size=1000
):
    """
    Version para archivos parquet. Reutiliza completamente el código existente.
    """
    hook_minio = get_minio_client(minio_id)
    hook_postgres = get_db_hook(connection_id)
    
    values_placeholders = ', '.join(['%s' for _ in columns])
    insert_query = f"""
        INSERT INTO {table} ({', '.join(columns)}) 
        VALUES ({values_placeholders})
        ON CONFLICT (id) 
        DO UPDATE SET 
            tipometadato_id = EXCLUDED.tipometadato_id,
            detalle = EXCLUDED.detalle,
            fecha = EXCLUDED.fecha
    """
    
    try:
        # 1. DESCUBRIR ARCHIVOS PARQUET (igual que tu código original)
        logging.info(f"Buscando archivos parquet en: {path_pattern}")
        parquet_files = (
            hook_minio.list_keys(bucket_name=bucket_name, prefix=path_pattern) or []
        )
        parquet_files = [key for key in parquet_files if key.endswith(".parquet")]
        
        if not parquet_files:
            logging.warning(f"No se encontraron archivos parquet en: {path_pattern}")
            return 0
            
        parquet_files.sort()
        logging.info(f"Encontrados {len(parquet_files)} archivos parquet")
        
        total_processed = 0
        file_count = 0
        
        # 2. PROCESAR CADA ARCHIVO (reutilizando funciones existentes)
        for file_name in parquet_files:
            file_count += 1
            logging.info(f"Procesando archivo {file_count}/{len(parquet_files)}: {file_name}")
            file_processed = 0
            
            try:
                # 3. STREAMING POR CHUNKS (reutilizando stream_parquet_file_chunked)
                for chunk_documents in _stream_parquet_file_chunked(
                    hook_minio, bucket_name, file_name, read_chunk_size
                ):
                    if not chunk_documents:
                        continue
                    
                    # 4. INSERCIÓN EN LOTES (reutilizando insert_chunk_to_postgres)
                    chunk_inserted = _insert_chunk_to_postgres(
                        hook_postgres,
                        chunk_documents,
                        insert_query,
                        columns,
                        batch_size
                    )
                    
                    file_processed += chunk_inserted
                    total_processed += chunk_inserted
                    logging.info(f"  Chunk procesado: {chunk_inserted} registros")
                    
            except Exception as e:
                logging.error(f"Error procesando archivo {file_name}: {e}")
                continue
                
            logging.info(f"Archivo completado: {file_processed} registros")
            
        logging.info(f"Proceso completado: {total_processed} registros totales de {len(parquet_files)} archivos")
        return total_processed
        
    except Exception as e:
        logging.error(f"Error en proceso de inserción de metadatos: {e}")
        raise
