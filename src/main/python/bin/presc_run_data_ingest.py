import pyspark.sql

import logging.config
import logging

logging.config.fileConfig("../utils/logging_to_file.conf")
logger = logging.getLogger(__name__)


def load_file(spark: pyspark.sql.SparkSession, file_dir, file_format, header, inferSchema):
    try:
        logger.info("The load_file() function is started...")

        if file_format == 'parquet':
            df = spark.read.format(file_format) \
                .load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format). \
                options(header=header, inferSchema=inferSchema).load(file_dir)
    except Exception as exp:
        logger.error("Exception in load_file() please check the trace -" + str(exp), exc_info=exp)
        raise
    else:
        logger.info(f"{file_dir} is successfully loaded from load_file()")

    return df
