from servify.settings.logging import Logger

from .resolve_latest_file import resolve_latest_file


def sample_bytes(
    path: str,
    *,
    sample_bytes: int,
    log: Logger,
) -> bytes:
    """
    Lê uma amostra binária do arquivo resolvido a partir de um caminho ou padrão.

    A função é usada pelas rotinas de detecção de encoding, delimitador e estilo
    de aspas sem precisar carregar o arquivo completo em memória.
    """

    log.info(f"Obtendo amostra de bytes: {path}")

    arquivo = resolve_latest_file(
        path,
        log=log,
    )

    try:
        with open(arquivo, "rb") as f:
            rawdata = f.read(sample_bytes)

    except Exception as exc:
        raise ValueError(
            f"Não foi possível amostrar o arquivo '{arquivo}': {exc}"
        ) from exc

    log.debug(f"Lidos {len(rawdata)} bytes")

    return rawdata
