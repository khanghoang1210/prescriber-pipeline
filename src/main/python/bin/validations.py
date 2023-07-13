

def get_curr_date(spark):
    opDF = spark.sql("""select current_date""")
    print("Validate the Spark object by printing current date" + str(opDF.collect()))