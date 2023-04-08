# Import all the necessary modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date
import sys
import logging
import logging.config
logging.config.fileConfig(fname='../util/logging_to_file.conf')
def main():
    try:
        # Get Spark object
        logging.info('main() is started ...')
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate Spark Object
        get_curr_date(spark)

        # Initiate run_presc_data_ingestion Script
            # Load the City File
            # Load the Prescriber Fact File
            # Validate
            # Set up logging Configuration Mechanism
            # Set up Error Handling

        # Initiate run_presc_data_preprocessing Script
            # Perform data Cleaning Operations
            # Validate
            # Set up logging Configuration Mechanism
            # Set up Error Handling

        # Initiate run_presc_data_transform Script
            # Apply all the transformation Logics
            # Validate
            # Set up logging Configuration Mechanism
            # Set up Error Handling

        # Initiate run_presc_data_extraction Script
            # Validate
            # Set up logging Configuration Mechanism
            # Set up Error Handling
        logging.info("presc_run_pipeline is completed.")
    except Exception as exp:
        logging.error("Error Occured in main() method. Please check again to go to the respective module and fix it."+str(exp), exc_info=True)
        sys.exit(1)

# End of Applications Part 1
if __name__ == "__main__":
    logging.info("run presc pipeline is Started ... ")
    main()