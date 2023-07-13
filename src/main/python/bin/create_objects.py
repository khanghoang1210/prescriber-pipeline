from pyspark.sql import SparkSession


def get_spark_object(envn,appName):
    try:

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
        print("Name error in method - spark_curr_date(). Please check the Stack Trace, " + str(exp))
        raise
    except Exception as exp:
        print("Error in method - spark_curr_date(). Please check the Stack Trace,  " + str(exp))
    else:
        print("Spark object is created!!!")
    return spark