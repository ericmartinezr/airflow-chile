# Airflow Chile

Este repositorio contendrá pipelines con Airflow para datos de Chile.

Estado: trabajo en progreso (WIP).

## Iniciar ambiente

> astro dev start

## Reiniciar ambiente

> astro dev restart

## Variables

- Agregar variable `API_MP` con el ticket de MercadoPublico´

## Pools

- Agregar pool `mp_licitacion_detail_pool` con valor 1

## Conexiones

- Agregar conexión `airflow_cl_db` a PostgreSQL
- Agregar conexión `my_gcp_conn` a Google Cloud (ver referencias [2])

## Variables de ambiente (.env)

```ini
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=gs://my_gcp_conn@<bucket>/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_COMPRESSION=zip
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=2000
```

# Referencias

1. https://www.astronomer.io/docs/learn/custom-xcom-backend-strategies
2. https://www.astronomer.io/docs/learn/custom-xcom-backends-tutorial (GCP)
