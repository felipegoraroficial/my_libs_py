import pytest

import servify
from servify.src.commons.functions.read import servify_read


def test_date_normalization_parses_dd_mm_yyyy_values(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
            ('01/02/2024'),
            ('31/12/2023')
        AS data(data)
        """
    )

    result = servify.normalization(
        df,
        "date",
        columns=["data"],
        formato="dd/MM/yyyy",
    )

    values = [
        row["data"].strftime("%Y-%m-%d") for row in result.orderBy("data").collect()
    ]
    assert values == ["2023-12-31", "2024-02-01"]
