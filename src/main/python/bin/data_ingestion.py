
import logging
import logging.config
# Load the logginf configuration file
#logging.config.fileConfig(fname='../util/logging_to_file.conf')

logging.config.fileConfig(fname='src/main/python/util/logging_to_files.conf')
logger = logging.getLogger(__name__)



def load_files(spark, file_format, file_dir, header, inferSchema):
    try:
        logger.info("The load file function is started!!")
        if file_format == 'parquet':
            df = spark.\
                read.\
                format(file_format).\
                load(file_dir)
        elif file_format == 'csv':
            df = spark.\
            read .\
            format(file_format).\
            options(inferSchema=inferSchema).\
            options(header=header).\
            load(file_dir)
    except Exception as exp:
        logger.error("Error in the method - load_files(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The input file {file_dir} is loaded to the dataframe. The load_files() function is completed!!")
    return df