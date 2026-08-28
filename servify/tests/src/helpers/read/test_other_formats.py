import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

read_by_format_module = importlib.import_module(
    "servify.src.helpers.read.read_by_format"
)

read_by_format_fn = read_by_format_module.read_by_format


class Log:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_read_by_format_other_formats_and_unsupported(spark):
    spark = MagicMock()
    frame = MagicMock()
    frame.withColumn.return_value = frame
    spark.read.parquet.return_value = frame
    spark.read.format.return_value.load.return_value = frame
    spark.table.return_value = frame
    assert (
        read_by_format_fn(
            spark, file_format="parquet", path_validado="a.parquet", log=Log()
        )
        is frame
    )
    assert (
        read_by_format_fn(
            spark, file_format="delta", path_validado="a.delta", log=Log()
        )
        is frame
    )
    assert (
        read_by_format_fn(
            spark, file_format="table", path_validado="db.table", log=Log()
        )
        is frame
    )
    with pytest.raises(ValueError, match="not support"):
        read_by_format_fn(spark, file_format="xml", path_validado="a.xml", log=Log())
