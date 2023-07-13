# Import all necessary modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date

def main():
    # Get spark objects
    spark = get_spark_object(gav.envn, gav.appName)
    print("spark object is created!!")
    # Validate spark object
    get_curr_date(spark)

if __name__ == '__main__':  
    main()