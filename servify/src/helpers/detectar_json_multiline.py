from servify.settings.logging import Logger

from .obter_encoding import obter_encoding
from .resolve_latest_file import resolve_latest_file


def detectar_json_multiline(path: str, *, log: Logger) -> bool:

    log.info(f"Starting JSON multiline detection for file: {path}")

    arquivo_escolhido = resolve_latest_file(path, log=log)
    encoding_detectado = obter_encoding(arquivo_escolhido, log=log)

    try:
        with open(arquivo_escolhido, "r", encoding=encoding_detectado) as f:
            linhas = f.readlines()
        log.info(f"File {arquivo_escolhido} read successfully.")
    except Exception as e:
        log.error(f"Error reading file {arquivo_escolhido}: {e}")
        raise ValueError(f"Error reading file {arquivo_escolhido}: {e}") from e

    primeira_linha = linhas[0].strip()
    log.debug(f"First line for JSON multiline detection: {primeira_linha}")

    if primeira_linha.startswith("{") or (
        primeira_linha.startswith("[") and len(linhas) >= 1
    ):
        log.info(f"JSON multiline detected for file: {arquivo_escolhido}")
        return True
    log.info(f"JSON single line detected for file: {arquivo_escolhido}")
    return False
