# Airflow Chile

Repositorio de pipelines con [Apache Airflow](https://airflow.apache.org/) para datos públicos de Chile. Utiliza [Astro Runtime](https://www.astronomer.io/docs/astro/runtime-image-architecture) como base del entorno de ejecución.

Estado: **trabajo en progreso (WIP)**.

## Prerrequisitos

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Cuenta de Google Cloud Platform (para XCom backend en GCS)
- Base de datos PostgreSQL

## Iniciar ambiente

```bash
astro dev start
```

## Reiniciar ambiente

```bash
astro dev restart
```

## Estructura del proyecto

```
dags/
├── mercado_publico.py        # DAG principal
└── sql/
    └── mercadopublico/       # Consultas SQL para upsert de licitaciones
requirements.txt              # Dependencias de Python (proveedores de Airflow)
Dockerfile                    # Imagen base: Astro Runtime
.env                          # Variables de ambiente (no commitear)
```

## DAGs

### Mercado Público

Pipeline que extrae licitaciones publicadas en [Mercado Público](https://www.mercadopublico.cl/) y las almacena en PostgreSQL.

- **Schedule**: diario (`@daily`)
- **Fecha de inicio**: 2026-01-01
- **Tags**: `scrapping`, `mercadopublico`

**Flujo general:**

1. Consulta la API de Mercado Público para obtener los códigos de las licitaciones del día anterior.
2. Por cada código, consulta el detalle completo de la licitación (dynamic task mapping).
3. Los resultados exitosos se serializan y hacen upsert en PostgreSQL.
4. Los registros con error se envían a una tabla DLQ (_dead letter queue_) para revisión.

**Consideraciones técnicas:**

- Se incrementó [`max_map_length`](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#max-map-length) a 2000 debido al alto volumen de licitaciones diarias.
- La API tiene _rate limiting_ (~2 segundos entre consultas), por lo que se creó un pool dedicado con 1 slot para evitar consultas paralelas.
- Se habilitó un XCom backend en Google Cloud Storage para no sobrecargar la base de datos con datos de XCom.

## Configuración

### Variable

| Variable | Descripción                                          |
| -------- | ---------------------------------------------------- |
| `API_MP` | Ticket de autenticación de la API de Mercado Público |

### Pool

| Pool                        | Slots | Descripción                                                        |
| --------------------------- | ----- | ------------------------------------------------------------------ |
| `mp_licitacion_detail_pool` | 1     | Control de concurrencia para consultas a la API de Mercado Público |

### Conexiones

| Conexión        | Tipo         | Descripción                                     |
| --------------- | ------------ | ----------------------------------------------- |
| `airflow_cl_db` | PostgreSQL   | Base de datos destino para las licitaciones     |
| `my_gcp_conn`   | Google Cloud | Conexión a GCP (usada para XCom backend en GCS) |

### Variables de ambiente (`.env`)

Configurar las siguientes variables en el archivo `.env` en la raíz del proyecto:

```ini
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=gs://<conn_id>@<bucket>/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_COMPRESSION=zip
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=2000
AIRFLOW__CORE__MAX_MAP_LENGTH=2000
```

Reemplazar `<conn_id>` y `<bucket>` con los valores correspondientes de la conexión GCP y el bucket de GCS.

## Referencias

1. https://www.astronomer.io/docs/learn/custom-xcom-backend-strategies
2. https://www.astronomer.io/docs/learn/custom-xcom-backends-tutorial (GCP)

---

_Este archivo fue escrito inicialmente a mano y posteriormente mejorado con asistencia de IA._
