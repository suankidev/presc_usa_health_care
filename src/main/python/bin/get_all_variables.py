import os


### Set Environment Variable
if os.getenv('USER') == 'suankivmuser':
    os.environ['envn'] = 'PROD'
else:
    os.environ['envn'] = 'PROD'

os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'


###Get Envrionment Variable
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

### set other Variables

appName = "USA Prescriber Research Report"
current_path = os.getcwd()


if os.getenv('USER') == 'suankivmuser':
    # prod env
    staging_dim_city = "presc/staging/diminsion"
    staging_fact = "presc/staging/fact"
    output_city = "presc/output/diminsion"
    output_fact = "presc/output/fact"

else:
    staging_dim_city = current_path + "\..\..\staging\diminsion_city"
    staging_fact = current_path + "\..\..\staging\\fact"



