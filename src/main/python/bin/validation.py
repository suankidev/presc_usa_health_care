import logging
import logging.config

import pyspark.sql

logging.config.fileConfig("../utils/logging_to_file.conf")
logger = logging.getLogger(__name__)


# or
# logging.getLogger("validation")

def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date""");
        logger.info("Validate the spark session by printing" + str(opDF.collect()))
    except NameError as exp:
        logger.info("NameError in method get_curr_date() Please check the stack trace - " + str(exp), exc_info=True)
        raise
    except Exception as e:
        logger.info("NameError in method get_curr_date() Please check the stack trace - " + str(e), exc_info=True)
        raise
    else:
        logger.info("Spark Object is Validated")


def df_count(df: pyspark.sql.DataFrame, dfname):
    try:
        logger.info(f"The DataFrame validation by count df_count() is started for datafram {dfname}")
        df_count = df.count()

        logger.info(f"The DataFrame count is {df_count}")
    except Exception as exp:
        logger.error(f"Error in the method - df_count(). Please check the Stack Trace." + str(exp))
        raise
    else:
        logger.info(f"The DataFrame Validation by df_count() is completed.")


def df_top10_rec(df: pyspark.sql.DataFrame, dfname):
    try:
        logger.info(f"The DataFrame validation by top 10 rec df_top10_rec() is started for datafram {dfname}")
        logger.info(f"Top 10 records are:.")
        df_pandas =  df.limit(5).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error(f"Error in the method - df_top10_rec(). Please check the Stack Trace." + str(exp))
        raise
    else:
        logger.info(f"The DataFrame Validation by df_top10_rec() is completed.")


def df_print_schema(df,dfName):
    try:
        logger.info(f"The DataFrame Schema Validation for Dataframe {dfName}...")
        sch=df.schema.fields
        logger.info(f"The DataFrame {dfName} schema is: ")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - df_show_schema(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("The DataFrame Schema Validation is completed.")
