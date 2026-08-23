"""Descarga de mociones (PDF) desde el Senado de Chile."""
import logging
import os

import requests

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = (10, 60)
DOWNLOAD_DIR = "/tmp"


def download_mocion(data: dict, download_dir: str = DOWNLOAD_DIR) -> dict:
    """Descarga la moción asociada a una ley y retorna el registro con su ruta."""
    url = data["link_mocion"]
    id_proyecto = data["id_proyecto"]
    file_path = os.path.join(download_dir, f"mocion_{id_proyecto}.pdf")

    if os.path.exists(file_path):
        logger.info(f"El archivo {file_path} ya existe, se sobrescribirá.")
        os.remove(file_path)

    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        with open(file_path, "wb") as f:
            f.writelines(response.iter_content(chunk_size=8192))

    logger.info(f"Moción descargada correctamente en {file_path}")
    # Propaga el registro completo (id_proyecto/titulo) para el embed posterior.
    return {**data, "path": file_path}
