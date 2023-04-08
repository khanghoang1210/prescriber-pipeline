import logging
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date """)
        logger.info("Validate the Spark Object by printing Currrent Date - " + str(opDF.collect()))
    except NameError as exp:
        logger.error("NameError in the method - spark_curr_date(), Please check again." + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(), Please check again." + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark object is validated. Spark Object is ready.")