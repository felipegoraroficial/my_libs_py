import pytest

import servify
from servify.src.commons.functions.read import servify_read


def test_float_normalization_converts_decimal_formats(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
            ('1.234,56'),
            ('-2,50'),
            ('R$ 3.00'),
            (CAST(NULL AS STRING))
        AS data(valor)
        """
    )

    result = servify.normalization(df, "float", columns=["valor"])

    values = [float(row["valor"]) for row in result.orderBy("valor").collect()]
    assert values == [-2.5, 0.0, 3.0, 1234.56]
