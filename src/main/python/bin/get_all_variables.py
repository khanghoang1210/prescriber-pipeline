import os
import path
# Set enviroment variables
#os.environ['envn'] = 'TEST'
os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

# Get enviroment variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

# Set other variables
appName = "USA Prescriber Researcher Report"
current_path = os.getcwd()
# staging_dim_city = current_path + '/src/main/python/staging/dimension_city'
# fact = current_path + '/src/main/python/staging/fact'
staging_dim_city = 'PrescPipeline/staging/dimension_city'
fact = 'PrescPipeline/staging/fact'

