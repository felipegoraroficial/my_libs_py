from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame

from servify.settings import SparkConfig
from servify.settings.config.flags import LOG_ENABLED
from servify.settings.logging import Logger
from servify.src.helpers.normalization import (
    tratativa_datetype,
    tratativa_floattype,
    tratativa_inttype,
    tratativa_stringtype,
    tratativa_timestamptype,
)

__all__ = ["servify_normalization"]


class servify_normalization:
    """Fachada para normalização de colunas Spark por tipo de dado."""

    def __init__(self, spark=None, log_enabled: bool | None = None):
        self.spark = spark or SparkConfig.get_or_create_spark(app_name="servify")
        effective_log = LOG_ENABLED if log_enabled is None else log_enabled
        self.log = Logger(self.spark, show_logs=effective_log)

    def normalization(
        self,
        df: DataFrame,
        tipo: str,
        columns: Sequence[str] | None = None,
        formato: str | None = None,
    ) -> DataFrame:
        """Normaliza um DataFrame conforme o tipo e as colunas informadas."""

        if df is None:
            raise ValueError("DataFrame de entrada é obrigatório.")

        tipo_normalizado = tipo.strip().lower()
        tipos_validos = {
            "string",
            "strings",
            "int",
            "integer",
            "float",
            "date",
            "timestamp",
        }
        if tipo_normalizado not in tipos_validos:
            raise ValueError(
                f"Tipo de normalização inválido: {tipo}. "
                f"Opções: {sorted(tipos_validos)}"
            )

        selected_columns = list(columns) if columns is not None else None
        if tipo_normalizado in {"strings", "string"}:
            return tratativa_stringtype(df, self.log, selected_columns)

        if not selected_columns:
            raise ValueError(f"Informe 'columns' para a normalização do tipo '{tipo}'.")

        if tipo_normalizado in {"int", "integer"}:
            return tratativa_inttype(df, selected_columns, self.log)
        if tipo_normalizado == "float":
            return tratativa_floattype(df, selected_columns, self.log)

        if not formato:
            raise ValueError(f"Informe 'formato' para a normalização do tipo '{tipo}'.")
        if tipo_normalizado == "date":
            return tratativa_datetype(df, selected_columns, formato, self.log)
        return tratativa_timestamptype(df, selected_columns, formato, self.log)


normalization = servify_normalization
