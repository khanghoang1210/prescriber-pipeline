# Import all necessary modules
import sys
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from data_ingestion import load_files
from data_preprocessing import perform_data_clean
from data_transform import city_report, top_5_Prescribers
import logging
import logging.config
import os
from subprocess import PIPE, Popen

# Load the logginf configuration file
#logging.config.fileConfig(fname='../util/logging_to_file.conf')

logging.config.fileConfig(fname='../util/logging_to_files.conf')

def main():
    try:
        logging.info("main() is started!!")
        # Get spark objects
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate spark object
        get_curr_date(spark)
        # Ingest dimension file
        # for file in os.listdir(gav.staging_dim_city):
        #     print("file is " + file)
        #     file_dir = gav.staging_dim_city +  '/' + file
        #     print(file_dir)
        #     if file.split('.')[1] == 'csv':
        #         file_format = 'csv'
        #         header = gav.header
        #         inferSchema = gav.inferSchema
        #     elif file.split('.')[1] == 'parquet':
        #         file_format = 'parquet'
        #         header = 'NA'
        #         inferSchema = 'NA'

        # Load the dimension file
        file_dir = 'PrescPipeline/staging/dimension_city'
        proc = Popen(["hdfs", "dfs", "-ls", "-C", file_dir], stdout=PIPE, stderr=PIPE)
        (out,err) = proc.communicate()
        if 'parquet' in out.decode():
            file_format = 'parquet'
            inferSchema = 'NA'
            header = 'NA'
        elif 'csv' in out.decode():
            file_format = 'csv'
            inferSchema = gav.inferSchema
            header = gav.header
        # Validate dimesion file
        df_city = load_files(spark=spark, file_format=file_format, file_dir=file_dir, header=header, inferSchema=inferSchema)
        df_count(df_city,'df_city')
        df_top10_rec(df_city, 'df_city')
        # Ingest fact file
        # for file in os.listdir(gav.fact):
        #     print("file is " + file)
        #     file_dir = gav.fact +  '/' + file
        #     print(file_dir)
        # if file.split('.')[1] == 'csv':
        #     file_format = 'csv'
        #     header = gav.header
        #     inferSchema = gav.inferSchema
        # elif file.split('.')[1] == 'parquet':
        #     file_format = 'parquet'
        #     header = 'NA'
        #     inferSchema = 'NA'

        # Load fact file
        file_dir = 'PrescPipeline/staging/fact'
        proc = Popen(["hdfs", "dfs", "-ls", "-C", file_dir], stdout=PIPE, stderr=PIPE)
        (out,err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

         # Validate fact file
        df_fact = load_files(spark=spark, file_format=file_format, file_dir=file_dir, header=header, inferSchema=inferSchema)
        df_count(df_fact,'df_fact')
        df_top10_rec(df_fact, 'df_fact')

        # preprocessing data
        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')

        # Transfrom data city
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_top10_rec(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')

        # Transform data prescribers
        df_presc_final = top_5_Prescribers(df_fact_sel)
        df_top10_rec(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')

        logging.info("run_presc_pipeline is Compeleted.")
    except Exception as exp:
        logging.error("Error occured in the main() method. Please check the Stack Trace, " + str(exp), exc_info=True)

if __name__ == '__main__':  
    logging.info("run_presc_pipeline is started!!!")
    main()