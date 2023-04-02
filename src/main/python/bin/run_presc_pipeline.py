###import all the necessary modules
import sys

import get_all_variables as gav
from create_objects import get_spark_object
from validation import  get_curr_date
import logging.config
import logging
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

      logging.info("run_presc_pipline is completed")

  except Exception as exe:
      logging.error("Error occured in main method check stack trace"+str(exe), exc_info=True)
      sys.exit(1)

if __name__ == '__main__':
    logging.info("run_presc pipline is started")
    main()
