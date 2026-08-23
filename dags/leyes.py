"""Pipeline que extrae leyes del Senado de Chile y las almacena en PostgreSQL."""
from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import TaskGroup, TriggerRule, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from pendulum import datetime

from include.leyes.dlq import dlq_failure_callback
from include.leyes.download import download_mocion as _get_download_mocion
from include.leyes.embed import NoVectorsError
from include.leyes.embed import embed_key_insights as _get_embed_insights
from include.leyes.embed import embed_pdf as _get_embed_pdf
from include.leyes.embed import embed_text as _get_embed_text
from include.leyes.extract import (
    _extract_ley_key_insights as _get_extract_ley_key_insights,
)
from include.leyes.extract import (
    _extract_leyes_publicadas as _get_extract_leyes_publicadas,
)
from include.leyes.scrape import extract_ley as _extract_ley

TIMEOUT_RAPIDO = timedelta(minutes=2)
TIMEOUT_EMBEDDING = timedelta(minutes=10)


@dag(
    start_date=datetime(2026, 1, 1, tz="America/Santiago"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "emartinez",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "max_active_tis_per_dag": 2,
    },
    tags=["scrapping", "leyes"],
)
def ley_chile():

    @task(execution_timeout=TIMEOUT_RAPIDO)
    def extract_leyes_publicadas() -> list[dict]:
        """Extrae las leyes publicadas el día anterior (America/Santiago)."""
        local_date = get_current_context()["logical_date"].in_timezone(
            "America/Santiago")
        record_date = local_date.subtract(days=1).format("D/M/YYYY")
        leyes_publicadas = _get_extract_leyes_publicadas(record_date)
        if not leyes_publicadas:
            raise AirflowSkipException(
                f"No existen leyes publicadas para la fecha {record_date}")
        return leyes_publicadas

    @task(
        execution_timeout=TIMEOUT_RAPIDO,
        on_failure_callback=dlq_failure_callback,
    )
    def extract_ley(data: dict) -> dict:
        """Scrapea el texto de la norma desde el enlace BCN."""
        return {**data, "text": _extract_ley(data)}

    @task(
        execution_timeout=TIMEOUT_RAPIDO,
        on_failure_callback=dlq_failure_callback,
    )
    def extract_ley_key_insights(data: dict) -> dict:
        """Extrae los insights clave de la ley y arma los parámetros del upsert."""
        if not data.get("text"):
            raise ValueError("No se encontró el texto de la ley.")
        if not data.get("titulo"):
            raise ValueError("No se encontró el título de la ley.")

        key_insights = _get_extract_ley_key_insights(
            data["text"], data["titulo"])

        if not key_insights:
            raise ValueError("El modelo no retornó insights.")

        return {
            "id_proyecto": data["id_proyecto"],
            "titulo": data["titulo"],
            "key_insights": key_insights,
        }

    @task(
        execution_timeout=TIMEOUT_EMBEDDING,
        retries=2,
        on_failure_callback=dlq_failure_callback,
    )
    def embed_ley(data: dict) -> dict:
        """Vectoriza la ley y retorna los parámetros listos para el upsert."""
        try:
            return _get_embed_text(data)
        except NoVectorsError as e:
            raise AirflowSkipException(str(e))

    @task(
        execution_timeout=TIMEOUT_EMBEDDING,
        retries=2,
        on_failure_callback=dlq_failure_callback,
    )
    def embed_key_insight(data: dict) -> dict:
        """Vectoriza los puntos claves detectados por el modelo"""
        try:
            return _get_embed_insights(data)
        except NoVectorsError as e:
            raise AirflowSkipException(str(e))

    @task(
        execution_timeout=TIMEOUT_RAPIDO,
        retries=3,
        on_failure_callback=dlq_failure_callback,
    )
    def download_mocion(data: dict) -> dict:
        """Descarga la moción (PDF) asociada a la ley."""
        return _get_download_mocion(data)

    @task(
        execution_timeout=TIMEOUT_EMBEDDING,
        retries=2,
        on_failure_callback=dlq_failure_callback,
    )
    def embed_mocion(data: dict) -> dict:
        """Vectoriza la moción y retorna los parámetros listos para el upsert."""
        try:
            return _get_embed_pdf(data)
        except NoVectorsError as e:
            raise AirflowSkipException(str(e))

    def _upsert(task_id: str, sql: str):
        return SQLExecuteQueryOperator.partial(
            task_id=task_id,
            conn_id="airflow_cl_db",
            sql=sql,
            autocommit=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

    _leyes = extract_leyes_publicadas()

    # === Rama moción (PDF) ===
    with TaskGroup("mocion"):
        _descargas = download_mocion.expand(data=_leyes)
        _embeds_mocion = embed_mocion.expand(data=_descargas)
        _upsert(
            "upsert_leyes_mocion_embedding",
            "sql/leyes/upsert_leyes_mocion_embedding.sql",
        ).expand(parameters=_embeds_mocion)

    # === Rama texto de ley ===
    with TaskGroup("ley"):
        _textos = extract_ley.expand(data=_leyes)

        _insights = extract_ley_key_insights.expand(data=_textos)
        _embed_insights = embed_key_insight.expand(data=_insights)
        _upsert(
            "upsert_leyes_key_insights_embedding",
            "sql/leyes/upsert_leyes_key_insights_embedding.sql",
        ).expand(parameters=_embed_insights)

        _embeds_ley = embed_ley.expand(data=_textos)
        _upsert(
            "upsert_leyes_embedding",
            "sql/leyes/upsert_leyes_embedding.sql",
        ).expand(parameters=_embeds_ley)


ley_chile()
