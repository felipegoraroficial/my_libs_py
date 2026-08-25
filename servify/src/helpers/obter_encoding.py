import chardet

from servify.settings.logging import Logger

from .sample_bytes import sample_bytes


def obter_encoding(path: str, *, log: Logger) -> str:

    raw = sample_bytes(path, sample_bytes=1000000, log=log)

    log.info(f"Initialized encoding detection for file: {path}")

    try:

        result = chardet.detect(raw) or {}
        encoding_detectado: str = result.get("encoding") or "utf-8"

        conf = result.get("confidence")
        log.debug(
            f"Encoding detected: {encoding_detectado} with confidence: {conf} and language: {result.get('language')}"
        )
    except Exception as e:
        log.error(
            f"Error detecting encoding for file {path}: {e}",
            exc_info=True,
        )
        raise ValueError(f"Error detecting encoding for file {path}: {e}") from e

    log.info(
        f"Encoding detection completed for file: {path} - Encoding: {encoding_detectado}"
    )

    return encoding_detectado
