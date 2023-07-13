# Import all necessary modules
import sys
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date

def main():
    try:
        # Get spark objects
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate spark object
        get_curr_date(spark)


    except Exception as exp:
        print("Error occured in the main() method. Please check the Stack Trace, " + str(exp))

if __name__ == '__main__':  
    main()