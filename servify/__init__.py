import sys
from types import ModuleType

from pyspark.sql import DataFrame

from .settings.config import flags as _settings
from .src.commons import servify_normalization, servify_read

_reader: servify_read | None = None
_last_log_state: bool | None = None


def read_data(
    path: str,
    formato: str,
    **kwargs,
) -> DataFrame:
    global _reader, _last_log_state

    from .settings.config.flags import (  # pylint: disable=import-outside-toplevel
        LOG_ENABLED,
    )

    if _reader is None or _last_log_state != LOG_ENABLED:
        _reader = servify_read(log_enabled=LOG_ENABLED)
        _last_log_state = LOG_ENABLED

    return _reader.read_data(path, formato, **kwargs)


def normalization(df, tipo: str, **kwargs):
    """Normaliza colunas de um DataFrame Spark conforme o tipo informado."""

    normalizer = servify_normalization(
        spark=df.sparkSession, log_enabled=_settings.LOG_ENABLED
    )
    return normalizer.normalization(df, tipo, **kwargs)


def show_options() -> None:
    """Imprime as opções de configuração disponíveis e seus valores atuais."""

    print("Opções de configuração do servify (sf.<opção> = valor):")

    print(f"  LOG_ENABLED          (bool)              atual={_settings.LOG_ENABLED!r}")

    print(
        f"  PERSIST_LOGS         (bool)              atual={_settings.PERSIST_LOGS!r}"
    )

    print(
        "  PERSIST_LOG_MIN_LEVEL "
        f"(str, opções={_settings.VALID_LOG_LEVELS}) "
        f"atual={_settings.PERSIST_LOG_MIN_LEVEL!r}"
    )

    print(
        "  PERSIST_LOGS_CATALOG "
        "(str, obrigatório se PERSIST_LOGS=True) "
        f"atual={_settings.PERSIST_LOGS_CATALOG!r}"
    )

    print(
        "  PERSIST_LOGS_SCHEMA "
        "(str, obrigatório se PERSIST_LOGS=True) "
        f"atual={_settings.PERSIST_LOGS_SCHEMA!r}"
    )


class _ServifyModule(ModuleType):
    """
    Módulo customizado que expõe as flags de configuração diretamente como
    atributos setáveis (ex.: ``sf.LOG_ENABLED = True``), delegando validação e
    persistência ao módulo ``servify.settings.config.flags``.
    """

    @property
    def LOG_ENABLED(self) -> bool:
        return _settings.LOG_ENABLED

    @LOG_ENABLED.setter
    def LOG_ENABLED(self, value: bool) -> None:
        _settings.set_logging(bool(value))

    @property
    def PERSIST_LOGS(self) -> bool:
        return _settings.PERSIST_LOGS

    @PERSIST_LOGS.setter
    def PERSIST_LOGS(self, value: bool) -> None:
        _settings.set_persist_logs(
            bool(value),
            catalog=_settings.PERSIST_LOGS_CATALOG,
            schema=_settings.PERSIST_LOGS_SCHEMA,
        )

    @property
    def PERSIST_LOGS_CATALOG(self) -> str | None:
        return _settings.PERSIST_LOGS_CATALOG

    @PERSIST_LOGS_CATALOG.setter
    def PERSIST_LOGS_CATALOG(self, value: str) -> None:
        _settings.set_persist_logs(
            _settings.PERSIST_LOGS,
            catalog=value,
            schema=_settings.PERSIST_LOGS_SCHEMA,
        )

    @property
    def PERSIST_LOGS_SCHEMA(self) -> str | None:
        return _settings.PERSIST_LOGS_SCHEMA

    @PERSIST_LOGS_SCHEMA.setter
    def PERSIST_LOGS_SCHEMA(self, value: str) -> None:
        _settings.set_persist_logs(
            _settings.PERSIST_LOGS,
            catalog=_settings.PERSIST_LOGS_CATALOG,
            schema=value,
        )


sys.modules[__name__].__class__ = _ServifyModule

__all__ = [
    "read_data",
    "normalization",
    "servify_read",
    "servify_normalization",
    "show_options",
]
