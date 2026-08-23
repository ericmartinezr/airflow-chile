"""Extracción de leyes publicadas desde la API del Senado de Chile."""
import random
import time

import requests
from airflow.providers.google.cloud.hooks.gen_ai import GenAIGeminiAPIHook
from airflow.sdk import Variable

ENDPOINT = (
    "https://web-back.senado.cl/api/published_laws"
    "?sort=fecha_ingreso&order=desc"
    "&limit={LIMIT}&offset={OFFSET}"
    "&palabra_clave=&desde={DESDE}&hasta={HASTA}"
)
PAGE_LIMIT = 10
REQUEST_TIMEOUT = 30
PAGE_DELAY_RANGE = (1, 3)
GENERATIVE_MODEL = "gemini-2.5-flash"


def _normalizar_ley(raw: dict) -> dict:
    """Convierte un registro de la API a claves internas y descarta campos no usados."""
    titulo = raw.get("TITULO") or ""
    if titulo.startswith('"') and titulo.endswith('"'):
        titulo = titulo[1:-1]
    return {
        "id_proyecto": raw.get("ID_PROYECTO"),
        "titulo": titulo.strip(),
        "link_bcn": raw.get("LINK_BCN"),
        "link_mocion": raw.get("LINK_DOCUMENTO_MENSAJE_MOCION"),
    }


def _extract_leyes_publicadas(record_date: str) -> list[dict]:
    """Obtiene todas las leyes publicadas en `record_date` paginando la API."""
    session = requests.Session()
    leyes_publicadas: list[dict] = []
    vistos: set[str] = set()

    offset = 0
    while True:
        endpoint = ENDPOINT.format(
            DESDE=record_date,
            HASTA=record_date,
            LIMIT=PAGE_LIMIT,
            OFFSET=offset,
        )
        r = session.get(endpoint, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        leyes = r.json()["data"]["data"]

        if not leyes:
            break

        nuevas = []
        for ley in leyes:
            norma = _normalizar_ley(ley)
            id_proyecto = norma["id_proyecto"]
            if id_proyecto and id_proyecto in vistos:
                continue
            if id_proyecto:
                vistos.add(id_proyecto)
            nuevas.append(norma)
        leyes_publicadas.extend(nuevas)

        if len(leyes) < PAGE_LIMIT:
            break

        offset += len(leyes)
        time.sleep(random.uniform(*PAGE_DELAY_RANGE))

    return leyes_publicadas


def _extract_ley_key_insights(ley: str, titulo: str) -> str:
    """Extrae los insights clave de una ley usando la API de Gemini."""
    from google.genai import types

    gemini_api_key = Variable.get("GEMINI_API_KEY", default=None)
    if not gemini_api_key:
        raise RuntimeError("Variable GEMINI_API_KEY no configurada")

    contenido = f"Titulo:\n`{titulo}`\nLey:\n```\n{ley}\n```"

    hook = GenAIGeminiAPIHook(
        gemini_api_key=gemini_api_key,
        gcp_conn_id="my_gcp_conn"
    )
    client = hook.get_genai_client()
    response = client.models.generate_content(
        model=GENERATIVE_MODEL,
        contents=contenido,
        config=types.GenerateContentConfig(
            system_instruction=[
                "Eres un asistente legal experto en leyes chilenas. ",
                "Analiza la siguiente ley y extrae los insights clave, ",
                "resumiendo su contenido en un texto conciso y claro en formato Markdown.",
                "No agregues conversación ni texto no solicitado (ejemplo: 'Aquí tienes...', 'A continuación...')."
            ]
        ),
    )

    return (response.text or "").strip()
