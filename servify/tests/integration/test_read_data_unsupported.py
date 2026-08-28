import pytest

import servify
from servify.src.commons.functions.read import servify_read


def test_read_data_rejects_unsupported_format(spark):
    reader = servify_read(spark=spark)

    with pytest.raises(ValueError, match="file format"):
        reader.read_data("/tmp/fake_path", "xml")
