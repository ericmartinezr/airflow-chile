"""DLQ: registra fallos de tasks en la tabla leyes_error vía on_failure_callback."""
import logging
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# task fallido -> (task upstream con el contexto, clave del registro usada como payload)
FUENTES_DLQ = {
    "mocion.download_mocion": ("extract_leyes_publicadas", "link_mocion"),
    "mocion.embed_mocion": ("mocion.download_mocion", "path"),
    "ley.extract_ley": ("extract_leyes_publicadas", "link_bcn"),
    "ley.extract_ley_key_insights": ("ley.extract_ley", "text"),
    "ley.embed_ley": ("ley.extract_ley", "id_proyecto"),
}

SQL_DLQ = Path(__file__).resolve().parents[2] / "dags/sql/leyes/upsert_leyes_error.sql"


def _pull_registro(ti, upstream: str) -> dict | None:
    """Recupera el XCom del upstream correspondiente al índice mapeado actual."""
    registro = None
    if ti.map_index >= 0:
        registro = ti.xcom_pull(task_ids=upstream, map_indexes=ti.map_index)
    if registro is None:
        # El upstream no está mapeado: su fila vive en map_index = -1.
        candidato = ti.xcom_pull(task_ids=upstream)
        registro = candidato if isinstance(candidato, dict) else None
    return registro


def dlq_failure_callback(context) -> None:
    """Escribe en leyes_error reutilizando upsert_leyes_error.sql.

    Nunca lanza: un error del callback no debe enmascarar el fallo original.
    """
    try:
        ti = context["task_instance"]
        upstream, clave_payload = FUENTES_DLQ[ti.task_id]
        registro = _pull_registro(ti, upstream) or {}
        parametros = {
            "id_proyecto": registro.get("id_proyecto"),
            "task": ti.task_id,
            "error_message": str(context.get("exception")),
            "payload": registro.get(clave_payload),
        }
        PostgresHook(postgres_conn_id="airflow_cl_db").run(
            SQL_DLQ.read_text(), autocommit=True, parameters=parametros
        )
        logger.error("Registrado en DLQ: %s", parametros)
    except Exception:
        logger.exception("No se pudo registrar el error en leyes_error")
