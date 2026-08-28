from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
from pyspark.sql import types as T

from servify.settings.config import flags
from servify.settings.config.spark_config import SparkConfig
from servify.settings.logging.coercion import (
    coerce_log_value,
    coerce_to_date,
    coerce_to_float,
    coerce_to_int,
    coerce_to_string,
    coerce_to_timestamp,
)
from servify.src.helpers.read.csv.analisar_quote_for_path import analisar_quote_for_path
from servify.src.helpers.read.xslx.remove_header_rows import remove_header_rows


class Log:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_quote_analysis_and_remove_duplicate_headers(tmp_path, spark):
    path = tmp_path / "quoted.csv"
    path.write_text('"name","note"\n"a","line\ncontinued"\n', encoding="utf-8")
    result = analisar_quote_for_path(str(path), log=Log())
    assert result["quote_suggestion"] == '"'
    assert result["needs_multiline"] is True
    assert result["reason"] == "quoted_newline_detected"

    df = spark.sql(
        "SELECT * FROM VALUES ('name', 'x'), ('alice', 'y') AS data(name, value)"
    )
    clean = remove_header_rows(df, log=Log())
    assert [row.name for row in clean.collect()] == ["alice"]
    df_without = spark.sql("SELECT * FROM VALUES ('alice', 'x') AS data(name, value)")
    assert remove_header_rows(df_without, log=Log()).count() == 1
