
import logging
import logging.config

# Load the logginf configuration file
#logging.config.fileConfig(fname='../util/logging_to_file.conf')

logging.config.fileConfig(fname='src/main/python/util/logging_to_files.conf')
logger = logging.getLogger(__name__)


def get_curr_date(spark):
    try:
        opDF = spark.sql("""select current_date""")
        logger.info("Validate the Spark object by printing current date" + str(opDF.collect()))
    except NameError as exp:
        logger.error("Name error in method - spark_curr_date(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in method - spark_curr_date(). Please check the Stack Trace,  " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark object is validated. Spark object is ready.")