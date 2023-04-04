"""

#City report


Transformation logic
1. Calculate the number of zips in each city.
2. Calculate the number of distinct prescriber assigned for each city
3. Calculate ethe trx_cnt prescriber for each city
4. don not report a city in the final report if no prescriber is assigned to it.


Layout:

country name
state name
country name
city population
number of zips
prescriber counts
total trx counts
"""
import pyspark.sql

from pyspark.sql.functions import split, col, udf, count, countDistinct, sum, rank, row_number, dense_rank
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

import logging.config
import logging

logging.config.fileConfig("../utils/logging_to_file.conf")
logger = logging.getLogger(__name__)


@udf(returnType=IntegerType())
def column_split_count(zip):
    return len(zip.split(' '))


def city_report(df_city_sel: pyspark.sql.DataFrame, df_fact_sel: pyspark.sql.DataFrame):
    # df_city_sel.show()
    # df_fact_sel.show()
    logger.info("city_report() method started..")

    try:
        # 1. Calculate the number of zips in each city.
        df_city_split = df_city_sel.withColumn('zip_count', column_split_count(col('zips')))

        # 2. Calculate the number of distinct prescriber assigned for each city
        # 3. Calculate ethe trx_cnt prescriber for each city

        df_fac_presc_grp = df_fact_sel.groupby('presc_city', 'presc_state').agg(
            countDistinct('presc_id').alias('presc_count'),
            sum('trx_cnt').alias('trx_counts')
        )

        # 4
        # join condition
        join_cond = (df_city_split.city == df_fac_presc_grp.presc_city) & (
                df_city_split.state_id == df_fac_presc_grp.presc_state)

        final_city_report = df_city_split.join(df_fac_presc_grp, join_cond, 'inner') \
            .select('city', 'state_name', 'country_name', 'population', 'zip_count', 'presc_count', 'trx_counts')

    except Exception as exp:
        logger.error("Error in method city_report(), see the stack trace-" + str(exp), exc_info=exp)
        raise
    else:
        logger.info("City_report() method successfull completed final city report is created..")
    return final_city_report


def presc_report(df_city_sel: pyspark.sql.DataFrame, df_fact_sel: pyspark.sql.DataFrame):
    try:
        logger.info("presc_report() method started...")

        # top 5 presc with highest txn count per each state, consider 20 to 50 yrs of prescriber experience
        spec = Window.partitionBy('presc_state').orderBy(col('trx_cnt').desc())
        df_rank = df_fact_sel.where('years_of_exp >= 20 and  years_of_exp <= 30').withColumn('top_5',
                                                                                             dense_rank().over(spec))
        final_pesc_report = df_rank.where(df_rank.top_5 <= 5) \
            .select('presc_id', 'presc_full_name', 'presc_state', 'country_name', 'years_of_exp', 'trx_cnt',
                    'total_day_supply', 'total_drug_cost')

    except Exception as e:
        logger.info("exceptionin presc_report() . please check the stack trace - " + str(e), exc_info=e)
        raise
    else:
        logger.info("exceptionin presc_report() method is completed successfully..")

    return final_pesc_report


"""
+-------------+--------+--------------------+--------------------+----------+--------------------+
|         city|state_id|          state_name|         countyr_name|population|                zips|
+-------------+--------+--------------------+--------------------+----------+--------------------+
|     NEW YORK|      NY|            NEW YORK|            NEW YORK|  18713220|11229 11226 11225...|
|  LOS ANGELES|      CA|          CALIFORNIA|         LOS ANGELES|  12750807|90291 90293 90292...|

+-----------+-------------+-----------+-------------------+------------+--------------------+-------+----------------+---------------+------------+-------------------+
|   presc_id|   presc_city|presc_state|        presc_spclt|years_of_exp|           drug_name|trx_cnt|total_day_supply|total_drug_cost|country_name|    presc_full_name|
+-----------+-------------+-----------+-------------------+------------+--------------------+-------+----------------+---------------+------------+-------------------+
|-2147461179|    BREMERTON|         WA|Physician Assistant|          54|        TRAMADOL HCL|   24.0|             594|         124.36|         USA|   DANIELLE DAEHNKE|
"""
