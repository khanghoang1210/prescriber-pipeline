import logging
import logging.config
from pyspark.sql.functions import upper, lit


logging.config.fileConfig(fname='src/main/python/util/logging_to_files.conf')
logger = logging.getLogger(__name__)


def perform_data_clean(df1, df2):

    try:
        logger.info(f"perform_data_clean is started for dimension data...")
        # Clean dimension data
        df_city_sel = df1.select(
            upper(df1.city).alias("city"),
            df1.state_id,
            upper(df1.state_name).alias("state_name"),
            upper(df1.county_name).alias("county_name"),
            df1.population,
            df1.zips
        )
        # Clean fact data
        logger.info(f"perform_data_clean is started for fact data...")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"), df2.nppes_provider_last_org_name.alias("presc_lname"),
                                 df2.nppes_provider_first_name.alias("presc_fname"), df2.nppes_provider_city.alias("presc_city"),
                                 df2.nppes_provider_state.alias("presc_state"), df2.specialty_description.alias("presc_spclt"), df2.years_of_exp,
                                 df2.drug_name, df2.total_claim_count.alias("trx_cnt"), df2.total_day_supply, df2.total_drug_cost
                                 )
        
        df_fact_sel = df_fact_sel.withColumn("country_name", lit("USA"))

    except Exception as exp:
        logger.error("Error in the method perform_data_clean(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() is completed.")
    return df_city_sel, df_fact_sel

