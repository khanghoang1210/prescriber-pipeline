import logging
import logging.config
from pyspark.sql.functions import upper


logging.config.fileConfig(fname='src/main/python/util/logging_to_files.conf')
logger = logging.getLogger(__name__)


def perform_data_clean(df1):

    try:
        logger.info(f"perform_data_clean is started...")
        df_city_sel = df1.select(
            upper(df1.city).alias("city"),
            df1.state_id,
            upper(df1.state_name).alias("state_name"),
            upper(df1.county_name).alias("county_name"),
            df1.population,
            df1.zips
        )
    except Exception as exp:
        logger.error("Error in the method perform_data_clean(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() is completed.")
    return df_city_sel

