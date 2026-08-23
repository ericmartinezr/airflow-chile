"""Scraping del texto de la norma desde la Biblioteca del Congreso Nacional (BCN)."""
import logging

logger = logging.getLogger(__name__)


def extract_ley(data: dict) -> str:
    """Extrae el texto de la norma desde `link_bcn` usando un navegador headless."""
    from scrapling.fetchers import DynamicFetcher

    link = data["link_bcn"]

    fetcher = DynamicFetcher.fetch(
        link,
        headless=True,
        wait_selector="div[class=p]",
        wait_selector_state="visible",
        timeout=60000,
    )

    if not fetcher:
        raise ValueError(
            f"No se pudo obtener el contenido de la norma: {link}"
        )

    parrafos = [
        text.strip()
        for text in fetcher.css("div[class=p]::text").getall()
        if text.strip()
    ]

    if not parrafos:
        raise ValueError(f"No se encontraron párrafos en la norma: {link}")

    logger.info(f"Se encontraron {len(parrafos)} párrafos en la norma {link}")
    return "\n".join(parrafos)
