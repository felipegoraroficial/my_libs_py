from __future__ import annotations

# Third-party
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Local
from servify.logging import Logger

__all__ = [
    "ConfigError",
    "DataValidationError",
    "IoError",
    "commons_shared",
]


class ConfigError(Exception):
    """Exceção para erros de configuração."""


class DataValidationError(Exception):
    """Exceção para erros de validação de dados."""


class IoError(Exception):
    """Exceção para erros de entrada/saída."""


class commons_shared:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._log = None

        if not hasattr(self, "_commons_shared_initialized"):
            self._commons_shared_initialized = True

    @property
    def log(self):
        # pylint: disable=import-outside-toplevel
        from servify.servify_configs import LOG_ENABLED

        if self._log is None or self._log.show_logs != LOG_ENABLED:
            self._log = Logger(self.spark, show_logs=LOG_ENABLED)

        return self._log

    def aplicar_schema_df(self, df: DataFrame, schema: T.StructType) -> DataFrame:
        """
        Aplica um schema especificado a um DataFrame do Spark.
        """

        self.log.info("Applying schema to DataFrame")

        if df is None:
            self.log.error("Input DataFrame is None")
            raise ValueError("Input DataFrame is None")

        if not schema.fields:
            self.log.warning("Provided schema is empty, returning original DataFrame")
            return df

        try:

            df_temp = df.toDF(*[f.name for f in schema.fields])

            for field in schema.fields:
                df_temp = df_temp.withColumn(
                    field.name, F.col(field.name).cast(field.dataType)
                )

            self.log.info("Schema applied successfully")
            return df_temp

        except Exception as e:
            self.log.error(f"Error applying schema: {e}")
            raise ValueError(f"Error applying schema: {e}") from e
