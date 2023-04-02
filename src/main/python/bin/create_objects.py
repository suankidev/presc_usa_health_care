from pyspark.sql import SparkSession
import logging.config

# load the logging config file
logging.config.fileConfig("../utils/logging_to_file.conf")
logger = logging.getLogger("create_object")


def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() is started, The '{envn}' envn is used.")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = (SparkSession.builder
                 .master(master)
                 .appName(appName)
                 .getOrCreate())
    except NameError as exp:
        logger.error("NameError in get_spark_object() . Please check the stack trace " + str(exp), exc_info=exp)
        raise
    except Exception as exp:
        logger.error("Error in get_spark_object() . Please check the stack trace " + str(exp),exc_info=exp)
        raise
    else:
        logger.info("Spark Object is created...")

    return spark
