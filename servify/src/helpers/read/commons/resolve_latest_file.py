import glob
import os

from servify.settings.logging import Logger


def resolve_latest_file(path: str, *, log: Logger) -> str:

    log.debug(f"Resolving path: {path}")

    try:
        path_resolvido = path.replace("file:", "")
    except Exception as e:
        log.error(f"Error resolving path: {e}")
        raise ValueError(f"Error resolving path: {e}") from e

    if "*" in path_resolvido:
        arquivos = glob.glob(path_resolvido)
        log.debug(f"Found files with wildcard: {arquivos}")

        if not arquivos:
            log.error(f"No files found for path with wildcard: {path_resolvido}")
            raise FileNotFoundError(f"No files found for path: {path_resolvido}")

        arquivos.sort(key=os.path.getmtime, reverse=True)
        escolhido = arquivos[0]
        log.info(f"Latest file selected: {escolhido}")
        return escolhido

    log.info(f"Path resolved without wildcard: {path_resolvido}")
    return path_resolvido
