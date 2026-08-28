import pytest

import servify
from servify.src.commons.functions.read import servify_read


def test_int_normalization_converts_numeric_text_and_invalid_values(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
            ('10'),
            ('R$ 20'),
            ('abc'),
            (CAST(NULL AS STRING))
        AS data(valor)
        """
    )

    result = servify.normalization(df, "int", columns=["valor"])

    values = [row["valor"] for row in result.orderBy("valor").collect()]
    assert values == [0, 0, 10, 20]
