
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

def df_count(df, dfName):
    try:
        logger.info(f"The dataframe validation by count df_count() is started for datafram {dfName}...")
        df_count = df.count()
        logger.info(f"The data frame count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe validation by count df_count() is completed.")

def df_top10_rec(df, dfName):
    try:
        logger.info(f"The dataframe validation by count df_top10_rec() is started for datafram {dfName}...")
        logger.info(f"The dataframe top 10 records are: ")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method - df_top10_rec(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe validation by top 10 records df_top10_rec() is completed.")
