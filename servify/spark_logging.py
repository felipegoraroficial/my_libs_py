import logging
import os


def disable_spark_logs():
    os.environ["PYSPARK_LOG_LEVEL"] = "OFF"

    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("org.apache").setLevel(logging.ERROR)
    logging.getLogger("akka").setLevel(logging.ERROR)
