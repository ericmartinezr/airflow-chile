"""Vectorización de leyes: texto / PDF → chunks → embeddings Gemini."""
import json
import logging

from airflow.sdk import Variable

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "gemini-embedding-2"
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 20
EMBED_BATCH_SIZE = 25
# El SDK google-genai interpreta HttpOptions.timeout en MILISEGUNDOS.
# 600000 ms = 10 minutos por llamada.
REQUEST_TIMEOUT_MS = 600_000


class NoVectorsError(ValueError):
    """Se lanza cuando no hay contenido ni embeddings que almacenar."""


def _embed_chunks(contenidos: list[str], id_proyecto: str, titulo: str) -> dict:
    """Genera embeddings por chunk y retorna los parámetros listos para el upsert.

    Función compartida por ``embed_text`` y ``embed_pdf``. El retorno coincide
    con el formato de parámetros de ``sql/leyes/upsert_*.sql``
    (``{"chunks": "<json>"}``).
    """
    from google.genai import Client, types

    gemini_api_key = Variable.get("GEMINI_API_KEY", default=None)
    if not gemini_api_key:
        raise RuntimeError("Variable GEMINI_API_KEY no configurada")

    if not contenidos:
        raise NoVectorsError(
            f"No se pudo extraer contenido de la ley {id_proyecto}")

    # El SDK google-genai cierra su transporte httpx en __del__ si el Client se
    # usa como objeto temporal (googleapis/python-genai #1763). El context
    # manager mantiene el cliente vivo durante todas las llamadas. Desde 2.14 el
    # SDK ya no cierra clientes httpx ajenos (#1566), por lo que no se necesita
    # un transporte propio.
    embeddings: list = []
    with Client(
        api_key=gemini_api_key,
        vertexai=False,
        http_options=types.HttpOptions(timeout=REQUEST_TIMEOUT_MS),
    ) as gemini_client:
        for i in range(0, len(contenidos), EMBED_BATCH_SIZE):
            lote = contenidos[i:i + EMBED_BATCH_SIZE]
            logger.info(
                f"Embebiendo lote "
                f"{i // EMBED_BATCH_SIZE + 1} de "
                f"{(len(contenidos) - 1) // EMBED_BATCH_SIZE + 1} "
                f"({len(lote)} chunks)")
            # gemini-embedding-2 es un modelo multimodal: el SDK fusiona una
            # list[str] en un solo Content de varias partes y devuelve UN único
            # embedding por llamada. Cada chunk debe ir en su propio Content.
            lote_content = [
                types.Content(parts=[types.Part(text=c)]) for c in lote
            ]
            vectors = gemini_client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=lote_content,
            )
            embeddings.extend(
                vectors.embeddings if vectors else [])  # type: ignore

    if not embeddings:
        raise NoVectorsError(
            f"No se generaron vectores a partir de la ley {id_proyecto}")
    if len(embeddings) != len(contenidos):
        raise RuntimeError(
            f"La cantidad de embeddings ({len(embeddings)}) no coincide "
            f"con la de chunks ({len(contenidos)}) para la ley {id_proyecto}")

    logger.info(
        f"Se generaron {len(contenidos)} chunks y {len(embeddings)} vectores "
        f"a partir de la ley con id: {id_proyecto}")

    filas = [
        {
            "id_proyecto": id_proyecto,
            "chunk_index": i,
            "titulo": titulo,
            "contenido": contenidos[i],
            "embeddings": embeddings[i].values,
        }
        for i in range(len(contenidos))
    ]
    return {"chunks": json.dumps(filas, ensure_ascii=False)}


def embed_text(data: dict) -> dict:
    """Genera embeddings por chunk del texto plano de una ley (parámetros de upsert)."""
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text = data["text"]
    id_proyecto = data["id_proyecto"]
    titulo = data["titulo"]

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
    contenidos = splitter.split_text(text)
    return _embed_chunks(contenidos, id_proyecto, titulo)


def embed_pdf(data: dict) -> dict:
    """Genera embeddings por chunk de la moción (PDF), listos para upsert."""
    import pymupdf4llm
    from langchain_text_splitters import MarkdownTextSplitter

    id_proyecto = data["id_proyecto"]
    titulo = data["titulo"]

    md = pymupdf4llm.to_markdown(data["path"])
    splitter = MarkdownTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
    documents = splitter.create_documents([md])  # type: ignore
    contenidos = [doc.page_content for doc in documents]

    return _embed_chunks(contenidos, id_proyecto, titulo)


def embed_key_insights(data: dict) -> dict:
    """Genera embeddings del resumen de puntos importantes generados por el modelo"""
    from langchain_text_splitters import MarkdownTextSplitter

    id_proyecto = data["id_proyecto"]
    titulo = data["titulo"]
    key_insights = data["key_insights"]

    splitter = MarkdownTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
    chunks = splitter.split_text(key_insights)
    return _embed_chunks(chunks, id_proyecto, titulo)
