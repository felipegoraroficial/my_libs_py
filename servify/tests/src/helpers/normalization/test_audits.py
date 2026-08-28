from servify.src.helpers.normalization.date.date_normalization import tratativa_datetype
from servify.src.helpers.normalization.float.float_normalization import (
    tratativa_floattype,
)
from servify.src.helpers.normalization.int.int_normalization import tratativa_inttype
from servify.src.helpers.normalization.strings.strings_normalization import (
    tratativa_stringtype,
)
from servify.src.helpers.normalization.timestamp.timestamp_normalization import (
    tratativa_timestamptype,
)


class AuditLog:
    show_logs = True
    audit_sample_fraction = 1.0
    audit_validate_after_transform = True

    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_normalization_audits_invalid_values(spark):
    log = AuditLog()
    strings = spark.sql("SELECT * FROM VALUES ('ok'), (''), ('NaN') AS data(value)")
    assert tratativa_stringtype(strings, log).count() == 3

    integers = spark.sql("SELECT * FROM VALUES ('1'), ('bad') AS data(value)")
    assert tratativa_inttype(integers, ["value"], log).count() == 2

    floats = spark.sql("SELECT * FROM VALUES ('1,5'), ('bad') AS data(value)")
    assert tratativa_floattype(floats, ["value"], log).count() == 2

    dates = spark.sql(
        "SELECT * FROM VALUES ('01/02/2024'), ('02/03/2024') AS data(value)"
    )
    assert tratativa_datetype(dates, ["value"], "dd/MM/yyyy", log).count() == 2

    timestamps = spark.sql(
        "SELECT * FROM VALUES ('2024-01-01 00:00:00'), ('2024-02-02 00:00:00') AS data(value)"
    )
    assert (
        tratativa_timestamptype(
            timestamps, ["value"], "yyyy-MM-dd HH:mm:ss", log
        ).count()
        == 2
    )
