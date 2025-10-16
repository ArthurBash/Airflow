# 🚀 Proyecto ETL Orquestado con Apache Airflow y MinIO S3

Este proyecto implementa un **pipeline de datos ETL (Extracción, Transformación y Carga)** de alto rendimiento, orquestado mediante **Apache Airflow 3.0.1**.  
Su objetivo principal es la **replicación y normalización de colecciones de MongoDB** en una base de datos **PostgreSQL** para su análisis posterior en **Apache Superset**.

Este repositorio incluye únicamente el código fuente de los DAGs de Airflow que implementan el proceso ETL. La ejecución y orquestación se realizan en una instancia de Apache Airflow desplegada online, fuera del alcance de este repositorio. 

---

## 🧭 Descripción General y Flujo de Trabajo

El flujo se ejecuta **diariamente**, asegurando la **consistencia y actualización** de los datos.

---

## 🧩 Componentes Clave de la Arquitectura

- **Orquestador:** Apache Airflow 3.0.1  
- **Fuente de Datos:** MongoDB  
- **Almacenamiento Intermedio:** MinIO S3   
- **Destino Final:** PostgreSQL  
- **Lenguaje:** Python *(100% del repositorio)*  

---

## 🔄 Proceso ETL

1. **Extracción (Extract):**  
   Conexión a MongoDB mediante credenciales gestionadas por las *Airflow Connections*.  
   Se procesan las colecciones definidas en el archivo `config.json`.

2. **Transformación (Transform):**  
   - Limpieza y normalización de nombres de columnas.  
   - Cálculo de campos derivados (por ejemplo, `cantidad_metadatos`).  
   - Enriquecimiento de datos con columnas adicionales.

3. **Almacenamiento Temporal:**  
   Los datos transformados se guardan en formato **Parquet**, particionados en archivos de **50.000 registros**.  
   Estos archivos se almacenan en **MinIO S3**, organizados por **colección** y **fecha de ejecución**.

4. **Carga (Load):**  
   Los archivos Parquet se leen desde **MinIO S3** en *chunks* para una inserción eficiente en las tablas de **PostgreSQL**.

5. **Post-procesamiento:**  
   DAGs adicionales actualizan campos y precalculan métricas o indicadores, preparando las tablas para el análisis con **Apache Superset**.

---

## 🎯 Finalidad Práctica

El propósito del proyecto es **centralizar y normalizar** datos provenientes de MongoDB en PostgreSQL, además de **precalcular métricas** para optimizar la generación de dashboards en **Apache Superset**.

---

## ⚙️ Orquestación y Estructura

El entorno completo se ejecuta dentro de **Docker**, utilizando contenedores separados para:

- `airflow-scheduler`
- `airflow-webserver`
- `airflow-worker`
- `minio`
- `postgres`

Todos interconectados mediante una **red interna de Docker**.

El pipeline de Airflow está compuesto por **varios DAGs** coordinados mediante el operador `TriggerDagRunOperator`, garantizando una **ejecución secuencial y controlada**.

---

### 🗂️ Estructura de DAGs y Módulos

- **`sis_orquestador.py`** → DAG principal que controla la ejecución completa del proceso ETL.  
- **`sis_parte_X.py`** → DAGs parciales (enumerados del 0 al 28) correspondientes a colecciones específicas.  
- **Módulos Comunes:**  
  - `common.py`  
  - `minio_utils.py`  
  - `mongo_utils.py`  
  - `parquet.py`  
  - `shared_utils.py`  

Estos archivos gestionan configuraciones, conexiones y operaciones específicas del pipeline.

---

## 🔐 Manejo de Credenciales

Las credenciales de los servicios (**MongoDB**, **MinIO S3** y **PostgreSQL**) se administran de forma centralizada a través de las **Airflow Connections**, garantizando la seguridad y portabilidad del entorno.

---

## 🧱 Tecnologías Principales

| Componente | Versión / Descripción |
|-------------|-----------------------|
| Apache Airflow | 3.0.1 |
| MongoDB | Fuente de datos |
| MinIO S3 | Almacenamiento intermedio (formato Parquet) |
| PostgreSQL | Destino final de los datos |
| Python | Lenguaje principal |
| Docker | Entorno de despliegue |

---

## 📊 Resultado Esperado

Un entorno automatizado que garantiza:

- Ingesta y transformación diaria de colecciones de MongoDB.  
- Estructuración y carga de datos en PostgreSQL.  
- Acceso rápido y confiable a datos precalculados desde Apache Superset.  

---

✳️ **Autor:** *Arturo Wettstein*  
📅 **Versión:** 1.0  

---
