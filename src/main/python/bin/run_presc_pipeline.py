# Import all the neccessary modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date
import sys
def main():
    try:
        # Get Spark object
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate Spark Object
        get_curr_date(spark)
            # Set up logging Configuration Mechanism
            # Set up Error Handling


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
    except Exception as exp:
        print("Error Occured in main() method. Please check again to go to the respective module and fix it."+str(exp))
        sys.exit(1)

# End of Applications Part 1
if __name__ == "__main__":
    main()