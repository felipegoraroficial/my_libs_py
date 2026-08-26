from typing import Optional

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
    "iso8859-1": "iso-8859-1",
    "latin-1": "iso-8859-1",
    "latin1": "iso-8859-1",
    "windows-1252": "iso-8859-1",
    "cp1252": "iso-8859-1",
}


def obter_encoding(path: str, *, log: Logger) -> str:

    raw = sample_bytes(
        path,
        sample_bytes=1_000_000,
        log=log,
    )

    log.info(f"Initialized encoding detection for file: {path}")

    try:

        result = chardet.detect(raw) or {}

        encoding_detectado: Optional[str] = (
            (result.get("encoding") or "").strip().lower()
        )

        confidence = result.get("confidence") or 0.0

        log.debug(
            f"Encoding detected: {encoding_detectado} "
            f"with confidence: {confidence} "
            f"and language: {result.get('language')}"
        )

        encoding_normalizado = (
            SUPPORTED_ENCODINGS.get(encoding_detectado) if encoding_detectado else None
        )

        if encoding_normalizado:

            try:
                raw.decode(encoding_normalizado)

                encoding_detectado = encoding_normalizado

            except UnicodeDecodeError:

                log.warning(
                    f"Detected encoding "
                    f"'{encoding_normalizado}' "
                    f"could not decode sample content. "
                    f"Starting fallback validation."
                )

                encoding_detectado = None

        else:

            log.warning(
                f"Unsupported encoding detected "
                f"'{encoding_detectado}'. "
                f"Starting fallback validation."
            )

            encoding_detectado = None

        # Fallback genérico baseado apenas nos
        # encodings suportados pela biblioteca
        if encoding_detectado is None:

            candidatos = list(dict.fromkeys(SUPPORTED_ENCODINGS.values()))

            for candidato in candidatos:
                try:

                    raw.decode(candidato)

                    encoding_detectado = candidato

                    log.info(f"Fallback encoding selected: {encoding_detectado}")

                    break

                except UnicodeDecodeError:
                    continue

        if encoding_detectado is None:

            raise ValueError(
                "Unable to determine a valid encoding for the sampled content."
            )

    except Exception as e:

        log.error(
            f"Error detecting encoding for file {path}: {e}",
            exc_info=True,
        )

        raise ValueError(f"Error detecting encoding for file {path}: {e}") from e

    log.info(
        f"Encoding detection completed for file: {path} "
        f"- Encoding: {encoding_detectado}"
    )

    return encoding_detectado
