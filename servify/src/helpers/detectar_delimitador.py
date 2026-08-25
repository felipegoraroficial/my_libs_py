import csv
import re

from servify.settings.logging import Logger

from .obter_encoding import obter_enconding
from .resolve_latest_file import resolve_latest_file


def detectar_delimitador(path: str, *, log: Logger) -> str:

    arquivo_escolhido = resolve_latest_file(path, log=log)

    encoding_detectado = obter_enconding(path, log=log)

    log.info(f"Starting delimiter detection for file: {arquivo_escolhido}")

    try:
        with open(arquivo_escolhido, "r", encoding=encoding_detectado, newline="") as f:
            linha = f.readline()
            if not linha:
                log.warning(f"File is empty: {arquivo_escolhido}.")

            log.debug(f"first line read for delimiter detection: {linha.rstrip("\n")}")

    except Exception as e:
        log.error(f"Error reading file {arquivo_escolhido}: {e}", exc_info=True)
        raise ValueError(f"Error reading file {arquivo_escolhido}: {e}") from e

    delimitadores = [",", ";", "\t", "|"]

    contagem = {d: len(re.findall(re.escape(d), linha)) for d in delimitadores}
    log.debug(f"Delimiter counts: {contagem}")

    if all(c == 0 for c in contagem.values()):
        log.warning(
            f"No delimiters found in the first line of file: {arquivo_escolhido}. Trying csv.Sniffer...."
        )
        try:
            dialect = csv.Sniffer().sniff(linha, delimiters="," ";|")
            detected = dialect.delimiter
            log.info(f"Delimiter detected by csv.Sniffer: {detected}")
            return detected
        except Exception as e:
            log.error(
                f"csv.Sniffer failed to detect delimiter for file {arquivo_escolhido}: {e}",
                exc_info=True,
            )
            log.warning(f"Using default delimiter ',' for file: {arquivo_escolhido}.")
            return ","

    delimitador_detectado = max(contagem.items(), key=lambda kv: kv[1])[0]
    log.info(
        f"Delimiter detected: {delimitador_detectado} for file: {arquivo_escolhido}"
    )
    return delimitador_detectado
