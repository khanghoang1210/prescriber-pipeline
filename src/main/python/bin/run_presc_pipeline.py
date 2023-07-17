# Import all necessary modules
import sys
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date
import logging
import logging.config

# Load the logginf configuration file
#logging.config.fileConfig(fname='../util/logging_to_file.conf')

logging.config.fileConfig(fname='src/main/python/util/logging_to_files.conf')

def main():
    try:
        logging.info("main() is started!!")
        # Get spark objects
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate spark object
        get_curr_date(spark)

        logging.info("run_presc_pipeline is compeleted...")
    except Exception as exp:
        logging.error("Error occured in the main() method. Please check the Stack Trace, " + str(exp), exc_info=True)

if __name__ == '__main__':  
    logging.info("run_presc_pipeline is started!!!")
    main()