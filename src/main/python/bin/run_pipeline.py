from pyspark.sql import SparkSession
spark = SparkSession\
    .builder\
    .master('local')\
    .appName('Testing')\
    .getOrCreate()

print('Spark Object is created!!')
print(spark)
