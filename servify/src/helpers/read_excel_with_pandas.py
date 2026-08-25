import os

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from servify.settings.logging import Logger
from servify.src.commons.shared.core import Shared_Commons

from .remove_header_rows import remove_header_rows
from .sanitize_columns import sanitize_columns


def read_excel_with_pandas(
    *,
    spark: SparkSession,
    log: Logger,
    commons_shared: Shared_Commons,
    xlsx_path: str,
    schema: T.StructType,
    sheet_name: int = 0,
) -> DataFrame:
    """
    Lê um arquivo .xlsx em um DataFrame do Pandas on Spark API.
    """

    log.info(f"Reading .xlsx file: {xlsx_path} by pandas")

    try:
        df = pd.read_excel(
            xlsx_path,
            engine="openpyxl",
            header=0,
            sheet_name=sheet_name,
        )

        valid_mask = df.notna().any(axis=1)
        if not bool(valid_mask.all()):
            log.warning(f"All rows are empty in .xlsx file: {xlsx_path}")
            raise ValueError(f"All rows are empty in .xlsx file: {xlsx_path}")

        first_valid_pos = int(np.argmax(valid_mask.to_numpy()))

    except Exception as e:
        log.warning(f"Error reading .xlsx file {xlsx_path} by pandas: {e}")
        raise

    header_raw = df.iloc[first_valid_pos].tolist()
    safe_cols = sanitize_columns(header_raw, prefer_from_schema=schema)

    df = df.iloc[first_valid_pos + 1 :].dropna(how="all").reset_index(drop=True)

    for c in range(len(safe_cols)):
        col = df.columns[c]
        df[col] = (
            df[col]
            .where(~df[col].isna(), None)
            .map(lambda x: str(x) if x is not None else None)
        )

    if schema is None:
        df.columns = safe_cols

        log.info(
            "Using safe conversion mode to avoid internal Serverless Arrow errors."
        )

        string_schema = T.StructType(
            [T.StructField(c, T.StringType(), True) for c in df.columns]
        )

        try:
            sdf = spark.createDataFrame(df, schema=string_schema)
        except Exception as e:
            log.error("Spark Serverless failed to create DataFrame even in safe mode.")
            log.debug(f"Internal Spark error: {e}")
            raise

        sdf = sdf.select([F.col(c).cast("string").alias(c) for c in sdf.columns])
        sdf = sdf.withColumn("source_file", F.lit(os.path.basename(xlsx_path)))
        sdf = remove_header_rows(sdf, log=log)

        return sdf

    df.columns = safe_cols

    string_schema = T.StructType(
        [T.StructField(c, T.StringType(), True) for c in df.columns]
    )

    sdf = spark.createDataFrame(df, schema=string_schema)

    sdf = sdf.withColumn("source_file", F.lit(os.path.basename(xlsx_path)))

    sdf = commons_shared.aplicar_schema_df(sdf, schema)

    sdf = remove_header_rows(sdf, log=log)

    return sdf
