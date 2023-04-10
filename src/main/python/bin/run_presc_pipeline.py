# Import all the necessary modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
import sys
import logging
import logging.config
import os
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
        for file in os.listdir(gav.staging_dim_city):
            print('File is ' + file)
            file_dir = gav.staging_dim_city + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
        # Load the Prescriber Fact File
        for file in os.listdir(gav.staging_fact):
            print('File is ' + file)
            file_dir = gav.staging_fact + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)

        # Validate run_data_ingest script for city Dimension dataframe & Prescriber Fact dataframe
        df_count(df_city, dfName='df_city')
        df_top10_rec(df_city, dfName='df_city')

        df_count(df_fact, dfName='df_fact')
        df_top10_rec(df_fact, dfName='df_fact')

    # Initiate run_presc_data_preprocessing Script
        # Perform data Cleaning Operations for df_city & df_fact
        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)
        # Validate
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')

        # Print schema
        df_print_schema(df_fact_sel, 'df_fact_sel')

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
        logging.error("Error Occurred in main() method. Please check again to go to the respective module and fix it." + str(exp), exc_info=True)
        sys.exit(1)

# End of Applications Part 1
if __name__ == "__main__":
    logging.info("run presc pipeline is Started ... ")
    main()