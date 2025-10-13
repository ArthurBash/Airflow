from app.sis.common import parse_date,parse_time

from bson import ObjectId

def safe_str(value):
    """
    Convierte un valor a string de forma segura.
    - ObjectId → str
    - None → None
    - Otros tipos → str normal
    """
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    return str(value)


def generate_extra_parte_solicitudes(documents):
    """
    Genera un diccionario de columnas adicionales a partir de un documento de MongoDB
    para inserción en PostgreSQL o exportación a Parquet.
    """
    return {
        "fecha": parse_date(documents.get("fecha")),
        "fechaIngreso": parse_date(documents.get("fechaIngreso")),
        "fechaUltimoAvance": parse_date(documents.get("fechaUltimoAvance")),
        "afectadas_ids": ",".join(safe_str(x) for x in documents.get("afectadas", [])),
        "responsables_ids": ",".join(safe_str(x) for x in documents.get("responsables", [])),
        "tipolugar_id": safe_str(documents.get("lugar", {}).get("tipo")) if documents.get("lugar") else None,
        "area_id": safe_str(documents.get("area")),
        "canal_id": safe_str(documents.get("canal")),
        "subtipo_id": safe_str(documents.get("tipo")),
        "estado_id": safe_str(documents.get("estado")),
        "lugar_detalle": documents.get("lugar", {}).get("detalle"),
        "lugar": documents.get("lugar", {}).get("nombre"),
        "solicitante_id": safe_str(documents.get("solicitante")),
        "usuario_id": safe_str(documents.get("usuario")),
        "hora": parse_time(documents.get("hora")),
        "mongo_updated_at": parse_date(documents.get("updatedAt")),
        "mongo_created_at": parse_date(documents.get("createdAt")),
    }


def generate_extra_parte_responsables(documents):
    """
    Parte 3
    Genera columnas adicionales para la inserción en MySQL desde un documento de MongoDB.

    Args:
        documents (dict): Documento de MongoDB.

    Returns:
        dict: Diccionario con las columnas 'agrupamiento_id', 'updated_at', y 'created_at'.
    """
    return {
        "agrupamiento_id": documents.get("agrupamiento", None),
        "mongo_updated_at": parse_date(documents.get("updatedAt")),
        "mongo_created_at": parse_date(documents.get("createdAt"))
    }

def generate_extra_parte_avances(documents):
    """
    Parte 4
    Genera columnas adicionales para los avances a partir de documentos de MongoDB.
    """
    return {
        "fecha": parse_date(documents.get("fecha")),
        "cantidad_metadatos": str(len(documents.get('metadatos', []))),
        "solicitud_id": documents.get("peticion", None),
        "usuario_id": documents.get("usuario", None)
    }

def generate_extra_parte_personasafectada(documents):
    """
    Parte 5
    Genera columnas adicionales para la inserción en MySQL desde un documento de MongoDB.

    Args:
        documents (dict): Documento de MongoDB.

    Returns:
        dict: Diccionario con las columnas adicionales como 'provincia', 'localidad', 'direccion',
        'generoId', 'nacionalidad', 'edad', 'ocupacion', 'infoDemograficaId', 'updated_at', y 'created_at'.
    """
    return {
        "provincia": documents.get("domicilio", {}).get("provincia", None),
        "localidad": documents.get("domicilio", {}).get("localidad", None),
        "direccion": documents.get("domicilio", {}).get("direccion", None),
        "generoId": documents.get("infoDemografica", {}).get("generoId", None),
        "nacionalidad": documents.get("infoDemografica", {}).get("nacionalidad", None),
        "edad": documents.get("infoDemografica", {}).get("edad", None),
        "ocupacion": documents.get("infoDemografica", {}).get("ocupacion", None),
        "infoDemograficaId": documents.get("infoDemografica", {}).get("_id", None),
        "mongo_updated_at": parse_date(documents.get("updatedAt")),
        "mongo_created_at": parse_date(documents.get("createdAt"))
    }

def generate_extra_parte_solicitante(documents):
    """
    Genera columnas adicionales para la inserción en MySQL desde un documento de MongoDB.

    Args:
        documents (dict): Documento de MongoDB.

    Returns:
        dict: Diccionario con la columna adicional 'tipoSolicitante' extraída de "__t".
    """
    return {
        "tipoSolicitante": documents.get("__t", None),
        "mongo_updated_at": parse_date(documents.get("updatedAt")),
        "mongo_created_at": parse_date(documents.get("createdAt"))
    }
