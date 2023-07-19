
from pyspark.sql.functions import upper, size, countDistinct, sum
from pyspark.sql.window import Window
import logging
import logging.config
from udfs import column_split_cnt


logging.config.fileConfig(fname='src/main/python/util/logging_to_files.conf')
logger = logging.getLogger(__name__)

def city_report(df_city_sel, df_fact_sel):
    try:
        # # City report
        # # Transform logics
        # 1. Calculate the number of zips in each city
        # 2. calculate the number of distinct prescribers assigned for each city
        # 3. calculate total trx_cnt prescribed for each city
        # 4. Do not report a city in the final report if no prescribers is assigned to it
        # # Layout:
        # City_name
        # State_name
        # County_name
        # City_population
        # Number of zips
        # Prescriber_counts
        # Total_trx_cnt
        df_city_split = df_city_sel.withColumn('zip_counts', column_split_cnt(df_city_sel.zips))
        df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city).agg(countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        df_city_join = df_city_split.join(df_fact_grp,(df_city_split.state_id == df_fact_grp.presc_state)&(df_city_split.city == df_fact_grp.presc_city),'inner')
        df_city_final = df_city_join.select("city", "state_name", "county_name","population", "zip_counts", "trx_counts", "presc_counts")
    except Exception as exp:
        logger.error("Error in the method - city_report(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transform city_report() is completed.")
        return df_city_final