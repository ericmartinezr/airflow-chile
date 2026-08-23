# Airflow Chile

Repositorio de pipelines con [Apache Airflow](https://airflow.apache.org/) para datos públicos de Chile. Utiliza [Astro Runtime](https://www.astronomer.io/docs/astro/runtime-image-architecture) como base del entorno de ejecución.

Estado: **trabajo en progreso (WIP)**.

## Tabla de contenidos

- [Prerrequisitos](#prerrequisitos)
- [Iniciar ambiente](#iniciar-ambiente)
- [Reiniciar ambiente](#reiniciar-ambiente)
- [Estructura del proyecto](#estructura-del-proyecto)
- [DAGs](#dags)
  - [Leyes de Chile](#leyes-de-chile)
  - [Mercado Público](#mercado-público)
- [Testing](#testing)
- [Referencias](#referencias)

## Prerrequisitos

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Cuenta de Google Cloud Platform (para XCom backend en GCS y autenticación de Gemini)
- Base de datos PostgreSQL con la extensión [pgvector](https://github.com/pgvector/pgvector) habilitada (necesaria para el DAG `leyes`)

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
├── leyes.py                    # DAG "leyes": leyes publicadas del Senado + embeddings
├── mercado_publico.py          # DAG "mercadopublico": licitaciones
└── sql/
    ├── leyes/                  # Consultas SQL para el DAG leyes
    │   ├── scripts.sql
    │   ├── upsert_leyes_embedding.sql
    │   ├── upsert_leyes_mocion_embedding.sql
    │   ├── upsert_leyes_key_insights_embedding.sql
    │   └── upsert_leyes_error.sql
    └── mercadopublico/         # Consultas SQL para upsert de licitaciones
        ├── scripts.sql
        ├── upsert_licitacion.sql
        └── upsert_licitacion_dlq.sql
tests/
└── dags/                       # Tests de estructura y lógica de los DAGs
requirements.txt                # Dependencias de Python (proveedores de Airflow)
Dockerfile                      # Imagen base: Astro Runtime + dependencias de Chromium
airflow_settings.yaml           # Config de conexiones/pools/variables para desarrollo local
.env                            # Variables de ambiente (no commitear)
```

## DAGs

### Leyes de Chile

Pipeline que extrae las **leyes publicadas del Senado de Chile** y las almacena en PostgreSQL con [PGVector](https://github.com/pgvector/pgvector). Por cada ley: scrapea el **texto de la norma**, genera un **resumen de puntos clave** con Gemini, descarga la **moción** asociada (PDF) y vectoriza los tres contenidos para hacerlos buscables por similitud. Los errores se registran en una tabla DLQ.

- **Archivo**: `dags/leyes.py`
- **ID del DAG**: `ley_chile`
- **Schedule**: diario (`@daily`)
- **Fecha de inicio**: 2026-01-01
- **Zona horaria**: `America/Santiago` (el DAG corre con `catchup=False` y `max_active_runs=1`)
- **Tags**: `scrapping`, `leyes`
- **Lógica**: módulos en `include/leyes/` (extract, scrape, download, embed, dlq)

**Flujo general:**

1. Consulta la API de leyes publicadas del Senado (`https://web-back.senado.cl/api/published_laws`) para obtener las leyes del **día anterior** a la fecha lógica de ejecución. Si no hay leyes, el resto del pipeline se omite (skipped).
2. A partir de esa lista, dos ramas trabajan en paralelo (dynamic task mapping por ley):
   - **Rama texto de la ley** (grupo `ley`): scrapea el texto de la norma desde BCN, genera el resumen de puntos clave con Gemini, y vectoriza tanto el texto completo como el resumen.
   - **Rama moción** (grupo `mocion`): descarga el PDF de la moción, lo convierte a Markdown, lo divide en fragmentos y lo vectoriza.
3. Cada rama hace **upsert** de sus embeddings en PostgreSQL (una fila por chunk).

#### Tareas

| Tarea                                 | Qué hace                                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------------------- |
| `extract_leyes_publicadas`            | Revisa qué leyes se publicaron ayer en el Senado y arma la lista de trabajo del día.         |
| `extract_ley`                         | Entra a la ficha de cada ley en la Biblioteca del Congreso y obtiene su texto completo.      |
| `extract_ley_key_insights`            | Pide a Gemini un resumen con los puntos clave de cada ley.                                   |
| `embed_key_insight`                   | Convierte ese resumen en vectores para que sea buscable por similitud.                       |
| `upsert_leyes_key_insights_embedding` | Guarda o actualiza los vectores del resumen en PostgreSQL.                                   |
| `embed_ley`                           | Divide el texto completo de cada ley en fragmentos y los convierte en vectores.              |
| `upsert_leyes_embedding`              | Guarda o actualiza los vectores del texto de la ley en PostgreSQL.                           |
| `download_mocion`                     | Descarga el PDF de la moción asociada a cada ley.                                            |
| `embed_mocion`                        | Convierte el PDF de la moción en Markdown, lo divide en fragmentos y lo convierte en vectores. |
| `upsert_leyes_mocion_embedding`       | Guarda o actualiza los vectores de la moción en PostgreSQL.                                  |

Las tareas de la rama texto corren bajo el grupo `ley` (`ley.extract_ley`, etc.) y las de la rama moción bajo `mocion`. Si una ley falla, se reintenta automáticamente y, al agotar los reintentos, el error queda registrado en la tabla `leyes_error` sin afectar al resto de las leyes.

**Consideraciones técnicas:**

- La API del Senado pagina resultados (`limit`/`offset`); entre páginas se espera un intervalo aleatorio de 1-3 segundos para no saturar el servidor. Todas las llamadas `requests` usan `timeout`.
- El scraping del texto de la norma usa `scrapling` con `DynamicFetcher` (Chromium headless), por lo que la imagen requiere las dependencias del navegador instaladas en el `Dockerfile`.
- La vectorización sigue la cadena texto/PDF → Markdown o fragmentos → embeddings de `gemini-embedding-2` (3072 dimensiones, SDK `google.genai`), generados en **lotes de 25 chunks** y guardados **uno por chunk** (con su `contenido` e índice). Aplica a tres contenidos: texto de la ley (`MarkdownTextSplitter`, chunk_size=1000, overlap=20 → `leyes_chunk_embedding`), moción PDF (`pymupdf4llm` + `MarkdownTextSplitter` → `leyes_mocion_chunk_embedding`) y resumen de puntos clave (`leyes_key_insight_embedding`).
- Concurrencia: las tareas de scraping/descarga/vectorización usan `max_active_tis_per_dag=2` (definido en los `default_args` del DAG) para no saturar el servidor del Senado ni la memoria de los workers.
- Las tareas que **no encuentran datos** (sin leyes publicadas, sin vectores generables) se marcan como **skipped** (`AirflowSkipException`). Los **errores** (red, API key, scrape, parse) hacen fallar la tarea —con sus reintentos— y al agotarlos un callback `on_failure_callback` (`include/leyes/dlq.py`) registra la fila en la tabla DLQ `leyes_error`, sin afectar al resto de las leyes.
- Los registros de la API se normalizan al entrar al pipeline (`_normalizar_ley`): claves internas en minúsculas (`id_proyecto`, `titulo`, `link_bcn`, `link_mocion`) y campos no utilizados descartados.
- Los PDFs de las mociones se descargan a `/tmp/mocion_{id_proyecto}.pdf`.

#### Configuración

##### Variable

| Variable         | Descripción                                     |
| ---------------- | ----------------------------------------------- |
| `GEMINI_API_KEY` | API Key de Gemini, obtenida de Google AI Studio |

##### Conexiones

| Conexión        | Tipo         | Descripción                                        |
| --------------- | ------------ | -------------------------------------------------- |
| `airflow_cl_db` | PostgreSQL   | Base de datos destino para los embeddings de leyes |
| `my_gcp_conn`   | Google Cloud | Conexión a GCP (autenticación de Gemini)           |

##### Base de datos

Las tablas se crean con `dags/sql/leyes/scripts.sql` y requieren la extensión **pgvector** habilitada en PostgreSQL:

| Tabla                          | Descripción                                                                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------- |
| `leyes_chunk_embedding`        | Un embedding (vector de 3072 dims) **por chunk** del texto de la ley, PK `(id_proyecto, chunk_index)` |
| `leyes_key_insight_embedding`  | Ídem, para los chunks del resumen de puntos clave generado por Gemini                                |
| `leyes_mocion_chunk_embedding` | Ídem, para los chunks de la moción (PDF)                                                             |
| `leyes_error`                  | DLQ de errores por `(id_proyecto, task)`                                                              |

Cada tabla tiene su upsert asociado en `dags/sql/leyes/` (`upsert_leyes_embedding.sql`, `upsert_leyes_key_insights_embedding.sql`, `upsert_leyes_mocion_embedding.sql`); todos insertan o actualizan por `(id_proyecto, chunk_index)` vía `jsonb_array_elements`. Los errores se persisten con `upsert_leyes_error.sql`, reutilizado por el callback de DLQ.

##### Dependencias

Dependencias específicas de este DAG (declaradas en `requirements.txt`):

- `scrapling[fetchers]` — scraping con navegador headless (Chromium)
- `pymupdf` / `pymupdf4llm` — extracción de texto de PDFs y conversión a Markdown
- `langchain` / `langchain-text-splitters` — división del texto en fragmentos
- `apache-airflow-providers-postgres` / `apache-airflow-providers-google` — hooks de PostgreSQL y Gemini

#### Estado y pendientes

> Sección de seguimiento para este DAG en desarrollo. Actualizar a medida que evolucione el código.

| Estado | Ítem                                                                                              |
| ------ | -------------------------------------------------------------------------------------------------- |
| ✅     | Extracción de leyes publicadas desde la API del Senado                                             |
| ✅     | Scraping del texto de la norma y su vectorización (`leyes_chunk_embedding`)                        |
| ✅     | Descarga y vectorización de la moción (`leyes_mocion_chunk_embedding`)                             |
| ✅     | Resumen de puntos clave con Gemini y su vectorización (`leyes_key_insight_embedding`)              |
| ✅     | DLQ vía `on_failure_callback` (`leyes_error`), concurrencia (`max_active_tis_per_dag`), timezone/catchup |
| ✅     | Tests del DAG e includes (`tests/dags/test_leyes.py`, `tests/include/test_embed.py`)               |

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

#### Configuración

##### Variable

| Variable | Descripción                                          |
| -------- | ---------------------------------------------------- |
| `API_MP` | Ticket de autenticación de la API de Mercado Público |

##### Pool

| Pool                        | Slots | Descripción                                                        |
| --------------------------- | ----- | ------------------------------------------------------------------ |
| `mp_licitacion_detail_pool` | 1     | Control de concurrencia para consultas a la API de Mercado Público |

##### Conexiones

| Conexión        | Tipo         | Descripción                                     |
| --------------- | ------------ | ----------------------------------------------- |
| `airflow_cl_db` | PostgreSQL   | Base de datos destino para las licitaciones     |
| `my_gcp_conn`   | Google Cloud | Conexión a GCP (usada para XCom backend en GCS) |

##### Variables de ambiente (`.env`)

Configurar las siguientes variables en el archivo `.env` en la raíz del proyecto:

```ini
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=gs://<conn_id>@<bucket>/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_COMPRESSION=zip
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=2000
AIRFLOW__CORE__MAX_MAP_LENGTH=2000
```

Reemplazar `<conn_id>` y `<bucket>` con los valores correspondientes de la conexión GCP y el bucket de GCS.

## Testing

Los tests se ejecutan con `pytest` y validan la carga, estructura y lógica de los DAGs:

```bash
pytest tests/
```

## Referencias

1. https://www.astronomer.io/docs/learn/custom-xcom-backend-strategies
2. https://www.astronomer.io/docs/learn/custom-xcom-backends-tutorial (GCP)
3. https://www.senado.cl — API de leyes publicadas (`https://web-back.senado.cl/api/published_laws`)
4. https://ai.google.dev/gemini-api/docs/embeddings — embeddings de Gemini
5. https://github.com/pgvector/pgvector — extensión PGVector para PostgreSQL
6. https://pymupdf.readthedocs.io/en/latest/rag.html — extracción de texto de PDFs (pymupdf4llm)
7. https://scrapling.readthedocs.io — scraping con navegador (scrapling)
8. https://www.astronomer.io/docs/astro/runtime-image-architecture — paquetes de proveedores incluidos en Astro Runtime

---

_Este archivo fue escrito inicialmente a mano y posteriormente mejorado con asistencia de IA._
