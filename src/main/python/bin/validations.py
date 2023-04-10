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

def df_count(df, dfName):
    try:
        logger.info(f"The DataFrame validation by count df_count() is started for DataFrame {dfName} ...")
        df_count = df.count()
        logger.info(f"The DataFrame count is {df_count}")
    except Exception as exp:
        logger.error("Error in method - df_count(). Please check the Stack Trace" + str(exp))
        raise
    else:
        logger.info("The DataFrame validation by count df_count() is completed.")

def df_top10_rec(df, dfName):
    try:
        logger.info(f"The DataFrame validation by top 10 record df_top10_rec() is started for DataFrame {dfName} ...")
        logger.info("The DataFrame top 10 record are: ")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index = False))
    except Exception as exp:
        logger.error("Error in method - df_top10_rec(). Please check the Stack Trace" + str(exp))
        raise
    else:
        logger.info("The DataFrame validation by count df_top10_rec() is completed.")

def df_print_schema(df, dfName):
    try:
        logger.info(f"DataFrame Schema validation for DataFrame {dfName}")
        sch = df.schema.fields
        logger.info(f"DataFrame Schema is {dfName}")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - df_print_schema(). Please check the Stack Trace." + str(exp),exc_info=True)
        raise
    else:
        logger.info("The DataFrame Schema validation is completed.")