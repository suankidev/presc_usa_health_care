import  logging
import  logging.config

logging.config.fileConfig("../utils/logging_to_file.conf")
logger = logging.getLogger(__name__)
#or
#logging.getLogger("validation")

def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date""");
        logger.info("Validate the spark session by printing"+str(opDF.collect()))
    except NameError as exp:
        logger.info("NameError in method get_curr_date() Please check the stack trace - "+ str(exp), exc_info=True)
        raise
    except Exception as e:
        logger.info("NameError in method get_curr_date() Please check the stack trace - " + str(e),exc_info=True)
        raise
    else:
        logger.info("Spark Object is Validated")


