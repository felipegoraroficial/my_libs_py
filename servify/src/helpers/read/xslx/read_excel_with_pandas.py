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
from .schema_inference import infer_schema


def read_excel_with_pandas(
    *,
    spark: SparkSession,
    log: Logger,
    commons_shared: Shared_Commons,
    xlsx_path: str,
    schema: T.StructType | None,
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
            header=None,
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

    data_start = first_valid_pos if schema is not None else first_valid_pos + 1
    df = df.iloc[data_start:].dropna(how="all").reset_index(drop=True)

    if schema is None:
        df.columns = safe_cols

        inferred_schema = infer_schema(df)
        log.info(f"Schema inferred automatically from XLSX: {inferred_schema}")

        for column, field in zip(df.columns, inferred_schema.fields):
            values = df[column].where(~df[column].isna(), None)
            if isinstance(field.dataType, T.StringType):
                values = values.map(
                    lambda value: str(value) if value is not None else None
                )
            df[column] = values

        try:
            sdf = spark.createDataFrame(df, schema=inferred_schema)
        except Exception as e:
            log.error("Spark failed to create DataFrame with inferred schema.")
            log.debug(f"Internal Spark error: {e}")
            raise

        sdf = sdf.withColumn("source_file", F.lit(os.path.basename(xlsx_path)))
        sdf = remove_header_rows(sdf, log=log)

        return sdf

    df.columns = safe_cols

    for c in range(len(safe_cols)):
        col = df.columns[c]
        df[col] = (
            df[col]
            .where(~df[col].isna(), None)
            .map(lambda x: str(x) if x is not None else None)
        )

    string_schema = T.StructType(
        [T.StructField(c, T.StringType(), True) for c in df.columns]
    )

    sdf = spark.createDataFrame(df, schema=string_schema)

    sdf = sdf.withColumn("source_file", F.lit(os.path.basename(xlsx_path)))

    sdf = commons_shared.aplicar_schema_df(sdf, schema)

    sdf = remove_header_rows(sdf, log=log)

    return sdf
