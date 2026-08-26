import chardet

from servify.settings.logging import Logger

from .sample_bytes import sample_bytes

SUPPORTED_ENCODINGS = {
    "utf-8": "utf-8",
    "ascii": "us-ascii",
    "us-ascii": "us-ascii",
    "utf-16": "utf-16",
    "utf-16le": "utf-16le",
    "utf-16be": "utf-16be",
    "utf-32": "utf-32",
    "iso-8859-1": "iso-8859-1",
    "latin-1": "iso-8859-1",
    "latin1": "iso-8859-1",
}


def obter_encoding(path: str, *, log: Logger) -> str:

    raw = sample_bytes(path, sample_bytes=1_000_000, log=log)

    log.info(f"Initialized encoding detection for file: {path}")

    try:

        result = chardet.detect(raw) or {}
        encoding_detectado = (result.get("encoding") or "utf-8").strip().lower()

        confidence = result.get("confidence") or 0.0

        log.debug(
            f"Encoding detected: {encoding_detectado} "
            f"with confidence: {confidence} "
            f"and language: {result.get('language')}"
        )

        if confidence < 0.70:
            log.warning(
                f"Low confidence ({confidence:.2f}) for encoding "
                f"'{encoding_detectado}'. Using utf-8."
            )
            encoding_detectado = "utf-8"

        encoding_detectado = SUPPORTED_ENCODINGS.get(
            encoding_detectado,
            "utf-8",
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
