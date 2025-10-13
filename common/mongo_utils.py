from airflow.providers.mongo.hooks.mongo import MongoHook
import logging
from bson import ObjectId
from pymongo import UpdateOne

def get_mongo_db(connection_id,database_name):
    """
    Obtiene una conexión a la base de datos de MongoDB y retorna la instancia de la base de datos.
    
    Args:
        connection_id (str): El ID de la conexión configurada en Airflow.
        database_name (str): El nombre de la base de datos a la que se desea conectar.

    Returns:
        Database: Objeto de la base de datos de MongoDB.

    """
    
    if not database_name:
        raise ValueError("El nombre de la base de datos no puede estar vacío.")
    
    try:
        logging.info(connection_id)
        mongo = MongoHook(conn_id=connection_id)
        client = mongo.get_conn()
        db = client[database_name]
        return db
    except Exception as e:
        logging.error(f"Error al conectar con la base de datos MongoDB en el módulo {get_mongo_db.__name__}: {e}")
        raise

def clear_collections(connection_id, database_name):
    """
    Vacia las colecciones específicas en MongoDB en el entorno de desarrollo.

    Args:
        connection_id (str): El ID de la conexión de MongoDB.
        database_name (str): El nombre de la base de datos en MongoDB.
    """
    collections_to_clear = ['detenidos_lvi', 'coleccion_cien', 'coleccion_alta_coincidencia', 'coleccionBajaCoincidencia']
    db = get_mongo_db(connection_id, database_name)
    
    for collection in collections_to_clear:
        db[collection].delete_many({})  # Eliminar todos los documentos de la colección
        logging.info(f'Colección {collection} vaciada en el entorno de desarrollo.')

def get_mongo_collection(collection, connection_id, database_name, filter=None, projection=None):
    """
    Obtiene documentos de una colección en MongoDB, con la posibilidad de aplicar un filtro.

    Args:
        collection (str): El nombre de la colección en MongoDB.
        connection_id (str): El ID de la conexión de MongoDB.
        database_name (str): El nombre de la base de datos en MongoDB.
        filter (dict, optional): Filtro para aplicar a la consulta. Si no se proporciona, se obtienen todos los documentos.
        projection (dict, optional): Proyección de campos a devolver. Si no se proporciona, se devuelven todos los campos.


    Returns:
        list: Lista de documentos de la colección.
    """
    # clear_collections(connection_id, database_name)
    if not collection:
        raise ValueError("El nombre de la colección no puede estar vacío.")
    if not connection_id:
        raise ValueError("El ID de la conexión no puede estar vacío.")
    if not database_name:
        raise ValueError("El nombre de la base de datos no puede estar vacío.")
    
    if filter is not None and not isinstance(filter, dict):
        raise ValueError("El filtro debe ser un diccionario.")
    if projection is not None and not isinstance(projection, dict):
        raise ValueError("La proyección debe ser un diccionario.")
    
    try:
        db = get_mongo_db(connection_id, database_name)
        
        filter = filter or {}
        projection = projection or {}
        
        cursor = db[collection].find(filter, projection)
        documents = list(cursor)
        
        return documents
    except Exception as e:
        logging.error(f"Error en el módulo {get_mongo_collection.__name__} al obtener documentos de la colección {collection} en la base de datos {database_name}: {e}")
        raise

def store_in_mongo(data, collection, connection_id, database_name, batch_size=10000):
    """
    Almacena los datos en una colección de MongoDB.
    Si ya existe un documento con el mismo _id, lo actualiza.

    Args:
        data (list): Lista de diccionarios, donde cada diccionario representa un documento.
        collection (str): Nombre de la colección.
        connection_id (str): Identificador de la conexión a MongoDB.
        database_name (str): Nombre de la base de datos.
        batch_size (int): Número de operaciones por batch.

    Returns:
        bool: True si la operación fue exitosa, False en caso contrario.
    """

    try:
        mongo_db = get_mongo_db(connection_id, database_name)

        if not data:
            logging.warning(f"No hay datos para almacenar en la colección '{collection}'.")
            return False
        
        operations = []
        for doc in data:
            if 'id_unico' not in doc:
                logging.warning(f"Documento sin campo 'id_unico': {doc}")
                continue
            operations.append(
                UpdateOne(
                    {'id_unico': doc['id_unico']},  # Filtro de búsqueda
                    {'$set': doc},  # Acción de actualización
                    upsert=True  # Si no existe, crea el documento
                )
            )

        if operations:
            # Realizamos la escritura en lotes
            for i in range(0, len(operations), batch_size):
                batch_operations = operations[i:i + batch_size]
                try:
                    mongo_db[collection].bulk_write(batch_operations)
                    logging.info(f"Operación realizada para un lote de {len(batch_operations)} documentos.")
                except BulkWriteError as bwe:
                    logging.error(f"Error en la operación bulk_write para un lote: {bwe.details}")
                    return False
                except Exception as e:
                    logging.error(f"Error inesperado al realizar bulk_write: {e}")
                    return False
            return True
        else:
            logging.warning("No hay operaciones para ejecutar en MongoDB.")
            return False

    except Exception as e:
        logging.error(f"Error en el módulo {store_in_mongo.__name__} al almacenar datos en MongoDB: {e}")
        return False
