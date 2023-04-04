###import all the necessary modules
import os
import sys

import get_all_variables as gav
from create_objects import get_spark_object
from src.main.python.bin.presc_run_data_preprocessing import perform_data_clean
from validation import get_curr_date, df_count, df_top10_rec, df_print_schema
import logging.config
import logging
import presc_run_data_ingest
from  presc_run_data_transform import city_report, presc_report

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
        for file in os.listdir(gav.staging_dim_city):
            print("file is - ", file)
            file_dir = gav.staging_dim_city + "\\" + file

            if file.split(".")[1] == "csv":
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split(".")[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        # load the file
        df_city = presc_run_data_ingest.load_file(spark=spark, file_dir=file_dir, file_format=file_format,
                                                  header=header, inferSchema=inferSchema)

        for file in os.listdir(gav.staging_fact):
            print("file is - ", file)
            file_dir = gav.staging_fact + "\\" + file

            if file.split(".")[1] == "csv":
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split(".")[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
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
        final_city_report = city_report(df_city_sel, df_fact_sel)

        # validate
        df_top10_rec(final_city_report, 'city_report')
        df_print_schema(final_city_report, 'city_report')


         #prescriber final report:
        final_presc_report = presc_report(df_city_sel, df_fact_sel)

        # validate
        df_top10_rec(final_presc_report, 'final_presc_report')
        df_print_schema(final_presc_report, 'final_presc_report')

        logging.info("run_presc_pipline is completed")

    except Exception as exe:
        logging.error("Error occured in main method check stack trace" + str(exe), exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    logging.info("run_presc pipline is started")
    main()
