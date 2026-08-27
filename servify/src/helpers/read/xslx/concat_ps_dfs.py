from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from servify.settings.logging import Logger

from .read_excel_with_pandas import read_excel_with_pandas


def concat_ps_dfs(
    *,
    spark: SparkSession,
    log: Logger,
    lista_files: List[str],
    schema: T.StructType,
    commons_shared,
) -> DataFrame:
    """
    Concatena uma lista de DataFrames do Pandas on Spark API.
    """

    if not lista_files:
        log.warning("No DataFrames provided for concatenation.")
        raise FileNotFoundError("No file provided for concatenation.")

    if len(lista_files) == 1:
        log.info("Only one file provided, returning it directly.")
        return read_excel_with_pandas(
            spark=spark,
            log=log,
            xlsx_path=lista_files[0],
            schema=schema,
            commons_shared=commons_shared,
        )

    log.info(f"Concatenating {len(lista_files)} Pandas on Spark DataFrames")

    df_final = read_excel_with_pandas(
        spark=spark,
        log=log,
        xlsx_path=lista_files[0],
        schema=schema,
        commons_shared=commons_shared,
    )
    log.info(f"file: {lista_files[0]} read successfully.")

    for file in lista_files[1:]:
        df = read_excel_with_pandas(
            spark=spark,
            log=log,
            xlsx_path=file,
            schema=schema,
            commons_shared=commons_shared,
        )
        df_final = df_final.unionByName(df, allowMissingColumns=True)
        log.info(f"file: {file} read and concatenated successfully.")

    log.info("DataFrames concatenated successfully.")
    return df_final
