from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from servify.settings.logging import Logger
from servify.src.commons.shared.core import Shared_Commons

from .commons.resolve_accessible_path import resolve_accessible_path
from .read_by_format import read_by_format
from .xslx.concat_ps_dfs import concat_ps_dfs
from .xslx.list_xlsx_paths import list_xlsx_paths

__all__ = ["HelperReadingData"]


class HelperReadingData:
    """
    Fachada que adapta as funções utilitárias de leitura de dados
    (xlsx/csv/json/parquet/delta/table) à interface esperada por
    ``servify_read``.
    """

    def __init__(
        self,
        spark,
        log_enabled: Optional[bool] = None,
    ):
        self.spark = spark

        # pylint: disable=import-outside-toplevel
        from servify.settings.config.flags import LOG_ENABLED

        effective_log = LOG_ENABLED if log_enabled is None else log_enabled

        self.log = Logger(
            spark,
            show_logs=effective_log,
        )

        self._commons_shared = Shared_Commons(spark)

    def list_xlsx_paths(
        self,
        path: str,
    ) -> List[str]:
        return list_xlsx_paths(
            path,
            log=self.log,
        )

    def concat_ps_dfs(
        self,
        paths: List[str],
        schema: T.StructType,
    ) -> DataFrame:
        return concat_ps_dfs(
            spark=self.spark,
            log=self.log,
            lista_files=paths,
            schema=schema,
            commons_shared=self._commons_shared,
        )

    def resolve_accessible_path(
        self,
        path: str,
        dbutils,
    ) -> str:
        return resolve_accessible_path(
            path,
            dbutils,
        )

    def read_by_format(
        self,
        file_format: str,
        path_validado: str,
    ) -> DataFrame:
        return read_by_format(
            self.spark,
            file_format=file_format,
            path_validado=path_validado,
            log=self.log,
        )
