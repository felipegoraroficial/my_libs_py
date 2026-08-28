import pytest

import servify
from servify.src.commons.functions.read import servify_read


def test_timestamp_normalization_parses_expected_format(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
            ('2024-01-02 03:04:05'),
            ('2024-01-03 08:09:10')
        AS data(created_at)
        """
    )

    result = servify.normalization(
        df,
        "timestamp",
        columns=["created_at"],
        formato="yyyy-MM-dd HH:mm:ss",
    )

    values = [
        row["created_at"].strftime("%Y-%m-%d %H:%M:%S")
        for row in result.orderBy("created_at").collect()
    ]
    assert values == ["2024-01-02 03:04:05", "2024-01-03 08:09:10"]
