import os


### Set Environment Variable
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'


###Get Envrionment Variable
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

### set other Variables

appName = "USA Prescriber Research Report"
current_path = os.getcwd()

staging_dim_city = current_path + "\staging\diminsion_city"
staging_fact = current_path + "\staging\\fact"


