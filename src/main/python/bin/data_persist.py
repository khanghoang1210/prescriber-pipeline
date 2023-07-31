import datetime as date
from pyspark.sql.functions import lit
import logging 
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_files.conf')
logger = logging.getLogger(__name__)

def data_persist_hive(spark, df, dfName, partitionBy, mode):
    try:
        logger.info(f"Data persist script - data_persist() is started for saving Dataframe {dfName} into Hive table...")
        # Add a static column with current date
        df = df.withColumn("delivery_date", lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql(""" create database if not exists prescpipeline location 'hdfs://localhost:9000/hive/user/warehouse/prescpipeline.db'""")
        spark.sql(""" use prescpipeline """)
        df.write.saveAsTable(dfName, partitionBy='delivery_date', mode=mode)
    except Exception as exp:
        logger.error("Error in the method data_persist(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"Data persist script - data_persist() is completed for saving Dataframe {dfName} into Hive table.")

def data_persist_postgre(spark, df, dfName, url, driver, dbtable, mode, user, password):
    try:
        logger.info(f"Data persist postgre is started for saving dataframe {dfName} into Postgre table")
        df.write.format("jdbc")\
                .option("url", url) \
                .option("driver", driver) \
                .option("dbtable", dbtable) \
                .mode(mode) \
                .option("user", user) \
                .option("password", password) \
                .save()
    except Exception as exp:
        logger.error("Error in method - data_persist_postgre(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"Data persist postgre is completed for saving dataframe {dfName} into Postgre table.")