import pytest

import servify
from servify.settings.config import flags


def test_normalization_handles_invalid_and_missing_numeric_values(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES ('-10'), ('abc'), (CAST(NULL AS STRING)) AS data(value)
    """
    )
    result = servify.normalization(df, "integer", columns=["value"])
    assert [row.value for row in result.orderBy("value").collect()] == [-10, 0, 0]
    with pytest.raises(ValueError, match="Colunas inexistentes"):
        servify.normalization(df, "float", columns=["missing"])
