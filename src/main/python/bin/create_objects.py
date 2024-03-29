from pyspark.sql import SparkSession
import logging
import logging.config

# Load the logginf configuration file
#logging.config.fileConfig(fname='../util/logging_to_file.conf')
logging.config.fileConfig(fname='../util/logging_to_files.conf')

logger = logging.getLogger(__name__)

def get_spark_object(envn,appName):
    try:
        logger.info(f"get_spark_object() is started. The '{envn}' envn is used")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession \
                .builder\
                .master(master)\
                .appName(appName)\
                .getOrCreate()
        
    except NameError as exp:
        logger.error("Name error in method - spark_curr_date(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise

    except Exception as exp:
        logger.error("Error in method - spark_curr_date(). Please check the Stack Trace,  " + str(exp), exc_info=True)
        
    else:
        logger.info("Spark object is created!!!")
    return spark