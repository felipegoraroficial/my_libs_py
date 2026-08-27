from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from servify.settings.logging import Logger


def remove_header_rows(spark_df: DataFrame, *, log: Logger) -> DataFrame:
    """
    Remove linhas de cabeçalho duplicadas de um DataFrame do Spark.
    """

    log.info("Verify if having heands on lines to remove")

    primeira_coluna = spark_df.columns[0]

    valor_normalizado = F.lower(
        F.regexp_replace(
            F.trim(F.regexp_replace(F.col(primeira_coluna), "_", " ")),
            r"\s+",
            " ",
        )
    )

    nome_normalizado = primeira_coluna.lower().replace("_", " ").strip()
    cond_header = valor_normalizado == F.lit(nome_normalizado)

    existe_header = len(spark_df.where(cond_header).take(1)) > 0
    df_limpo = spark_df.where(~cond_header)

    if existe_header:
        log.info("Header rows detected, removing them")
        return df_limpo

    log.info("No header rows detected, returning original DataFrame")
    return spark_df
