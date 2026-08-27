import os
from typing import List

from servify.settings.logging import Logger


def list_xlsx_paths(dir_path: str, *, log: Logger) -> List[str]:
    """
    Lista todos os arquivos .xlsx em um diretório especificado.
    """

    log.info(f"Listing .xlsx files in directory: {dir_path}")

    if os.path.isfile(dir_path):
        if dir_path.lower().endswith(".xlsx"):
            log.info(f"Single .xlsx file found: {dir_path}")
            return [dir_path]

        log.error(f"The specified path is a file but not .xlsx: {dir_path}")
        raise ValueError(f"The specified path is a file but not .xlsx: {dir_path}")

    if os.path.isdir(dir_path):
        paths = [
            os.path.join(dir_path, nome)
            for nome in os.listdir(dir_path)
            if nome.lower().endswith(".xlsx")
        ]
        if not paths:
            log.error(f"No .xlsx files found in directory: {dir_path}")
            raise FileNotFoundError(f"No .xlsx files found in directory: {dir_path}")

        log.info(f"Found {len(paths)} .xlsx files in directory: {dir_path}")
        return paths

    log.error(f"The specified path is neither a file nor a directory: {dir_path}")
    raise FileNotFoundError(
        f"The specified path is neither a file nor a directory: {dir_path}"
    )
