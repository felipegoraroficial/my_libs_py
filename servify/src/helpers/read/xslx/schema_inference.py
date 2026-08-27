import pandas as pd
from pyspark.sql import types as T


def infer_schema(df: pd.DataFrame) -> T.StructType:
    """Converte os tipos inferidos pelo Pandas em tipos compatíveis com Spark."""

    fields = []
    for column in df.columns:
        dtype = df[column].dtype
        data_type: T.DataType
        if pd.api.types.is_bool_dtype(dtype):
            data_type = T.BooleanType()
        elif pd.api.types.is_integer_dtype(dtype):
            data_type = T.LongType()
        elif pd.api.types.is_float_dtype(dtype):
            data_type = T.DoubleType()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            data_type = T.TimestampType()
        else:
            data_type = T.StringType()
        fields.append(T.StructField(str(column), data_type, True))

    return T.StructType(fields)
