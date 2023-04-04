###import all the necessary modules
import os
import sys

import get_all_variables as gav
from create_objects import get_spark_object
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_extraction import extract_files
from validation import get_curr_date, df_count, df_top10_rec, df_print_schema
import logging.config
import logging
import presc_run_data_ingest
from presc_run_data_transform import city_report, presc_report
import subprocess
from presc_run_data_persist import data_persist_hive, data_persist_postgre

###get all variables


###get spark object
# validate spark object
# setup logging config mechanism
# setup error handling

###Initate run_pesc_data_ingest script
# load the city file
# load the presciber fact file
# validate
# setup logging config
# setup error handling
logging.config.fileConfig("../utils/logging_to_file.conf")


def main():
    try:
        logging.info("main() is started")
        spark = get_spark_object(gav.envn, gav.appName)
        get_curr_date(spark)

        file_dir = "presc/staging/dimension"
        proc = subprocess.Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'
        elif 'csv' in out.decode():
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema

        # load the file
        df_city = presc_run_data_ingest.load_file(spark=spark, file_dir=file_dir, file_format=file_format,
                                                  header=header, inferSchema=inferSchema)
        file_dir = "presc/staging/fact"
        proc = subprocess.Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'
        elif 'csv' in out.decode():
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema
        # load fact file
        df_fact = presc_run_data_ingest.load_file(spark=spark, file_dir=file_dir, file_format=file_format,
                                                  header=header, inferSchema=inferSchema)

        #### Validate the run_data_ingest script for city diminsion dataframe
        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')

        #### Validate the run_data_ingest script for fact prescriber dataframe
        df_count(df_fact, 'df_fact')
        df_top10_rec(df_fact, 'df_fact')

        ### Initiate presc_rin_date_preprocessing script
        # perform data clenaing operation for df_city
        # 1. selec only required columns
        # conver city, state and country fields to upper case (b/c we need to join based on these field b/c fact is upper case)

        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)

        # validaiton
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')

        df_print_schema(df_city_sel, 'df_city_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')

        # transformation logic
        df_city_final = city_report(df_city_sel, df_fact_sel)

        # validate
        df_top10_rec(df_city_final, 'city_report')
        df_print_schema(df_city_final, 'city_report')

        # prescriber final report:
        df_presc_final = presc_report(df_city_sel, df_fact_sel)

        # validate
        df_top10_rec(final_presc_report, 'final_presc_report')
        df_print_schema(final_presc_report, 'final_presc_report')

        ### Initiate run_data_extraction Script
        CITY_PATH = gav.output_city
        extract_files(df_city_final, 'json', CITY_PATH, 1, False, 'bzip2')

        PRESC_PATH = gav.output_fact
        extract_files(df_presc_final, 'orc', PRESC_PATH, 2, False, 'snappy')

        ### Persist Data
        # Persist data at Hive
        data_persist_hive(spark=spark, df=df_city_final, dfName='df_city_final', partitionBy='delivery_date',
                          mode='append')
        data_persist_hive(spark=spark, df=df_presc_final, dfName='df_presc_final', partitionBy='delivery_date',
                          mode='append')

        # Persist data at Postgre
        data_persist_postgre(spark=spark, df=df_city_final, dfName='df_city_final',
                             url="jdbc:postgresql://localhost:6432/prespipeline", driver="org.postgresql.Driver",
                             dbtable='df_city_final', mode="append", user=gav.user, password=gav.password)

        data_persist_postgre(spark=spark, df=df_presc_final, dfName='df_presc_final',
                             url="jdbc:postgresql://localhost:6432/prespipeline", driver="org.postgresql.Driver",
                             dbtable='df_presc_final', mode="append", user=gav.user, password=gav.password)

        ### End of Application Part 1
        logging.info("presc_run_pipeline.py is Completed.")

        logging.info("run_presc_pipline is completed")

    except Exception as exe:
        logging.error("Error occured in main method check stack trace" + str(exe), exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    logging.info("run_presc pipline is started")
    main()
