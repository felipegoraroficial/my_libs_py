import pytest

import servify
from servify.settings.config import flags


def test_normalization_special_date_formats(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES ('jan/24'), ('dez/23'), ('01 de março de 2024')
        AS data(value)
    """
    )
    month_result = servify.normalization(
        df.limit(2), "date", columns=["value"], formato="MMM/yy"
    )
    assert [
        row.value.strftime("%Y-%m-%d")
        for row in month_result.orderBy("value").collect()
    ] == ["2023-12-01", "2024-01-01"]
    long_result = servify.normalization(
        spark.sql("SELECT 'sexta-feira, 01 de março de 2024' AS value"),
        "date",
        columns=["value"],
        formato="EEEE, dd 'de' MMMM 'de' yyyy",
    )
    assert long_result.first().value.strftime("%Y-%m-%d") == "2024-03-01"
