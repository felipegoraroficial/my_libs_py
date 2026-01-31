from __future__ import annotations

# Built-in
from typing import Optional

# Third-party
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Local
from app.helpers import helper_reading_data
from app.logging import Logger
from app.settings import EnvSparkSettings

__all__ = [
    "ConfigError",
    "DataValidationError",
    "IoError",
    "servify_read",
]


class ConfigError(Exception):
    """Exceção para erros de configuração."""


class DataValidationError(Exception):
    """Exceção para erros de validação de dados."""


class IoError(Exception):
    """Exceção para erros de entrada/saída."""


class servify_read:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.log = Logger(spark)
        self.settings = EnvSparkSettings(spark)
        self.helper_reading_data = helper_reading_data(spark)

        if not hasattr(self, "_reading_data_initialized"):
            self.log.info("Class Reading Data initialized")
            self._reading_data_initialized: bool = True

    def read_data(
        self, path: str, file_format: str, partition_column: Optional[str] = None
    ) -> DataFrame:
        """
        Lê dados de um path especificado, detectando encoding, delimitador e JSON multiline quando aplicável.
        """

        formatos_validos = {"csv", "txt", "json", "parquet", "delta", "table"}

        if file_format not in formatos_validos:
            self.log.error(f"file format '{file_format}' not support.")
            raise ValueError(f"file format '{file_format}' not support.")

        self.log.info(f"Starting data read for path: {path} with format: {file_format}")

        if partition_column:
            self.log.debug(f"Partition column: {partition_column}")
        else:
            self.log.debug("No partition column specified.")

        dbutils = self.settings.require_dbutils(self.spark)

        if file_format in {"csv", "txt", "json", "parquet", "delta"}:
            path_validado = self.helper_reading_data.resolve_accessible_path(
                path, dbutils
            )

        else:
            if not self.spark.catalog.tableExists(path):
                self.log.error(f"Table '{path}' does not exist in the catalog.")
                raise ValueError(f"Table '{path}' does not exist in the catalog.")

            path_validado = path
            self.log.info(f"Table '{path}' exists in the catalog.")

        self.log.info(f"Reading date from: {path_validado}")

        df = self.helper_reading_data.read_by_format(file_format, path_validado)

        if partition_column and file_format in {"delta", "table"}:
            if partition_column not in df.columns:
                self.log.error(
                    f"Partition column '{partition_column}' not found in data columns."
                )
                raise ValueError(
                    f"Partition column '{partition_column}' not found in data columns."
                )

            self.log.info(f"Filtering by last partitito from: {partition_column}")

            ultima_particao = df.select(
                F.max(F.col(partition_column)).alias("max_partition")
            ).collect()[0]["max_partition"]
            self.log.info(f"Last partition value: {ultima_particao}")
            df = df.filter(F.col(partition_column) == F.lit(ultima_particao))

        if df.isEmpty():
            self.log.warning(
                f"No data found in path: {path_validado} with format: {file_format}"
            )
            raise ValueError(
                f"No data found in path: {path_validado} with format: {file_format}"
            )

        self.log.info(f"Data read completed for path: {path_validado}")

        return df

    def read_xlsx(self, dir_path: str, schema: T.StructType) -> DataFrame:
        """
        Lê todos os arquivos .xlsx em um diretório e retorna um DataFrame do Spark com o schema especificado.
        """

        self.log.info(f"Reading .xlsx file in directory: {dir_path}")

        paths = self.helper_reading_data.list_xslx_paths(dir_path)

        df = self.helper_reading_data.concat_ps_dfs(paths, schema)

        self.log.info(
            f"All .xlsx files read and combined successfully from directory: {dir_path}"
        )

        return df
