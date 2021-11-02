# Read data from csv in S3 using Athena

import boto3
import pandas
import time

# Default profiles
client_athena = boto3.client('athena', region_name='us-east-1')
client_s3 = boto3.client('s3', region_name='us-east-1')

# Read config data from excel in S3
# try:
#     config_excel = client_s3.get_object(Bucket='ta-wpdata-data-validation-dev', Key='773_tables-landing-aws-2.xlsx')
#     config_df = pandas.read_excel(config_excel['Body'], sheet_name='Config')
#     df_queries_count = pandas.read_excel(config_excel['Body'], sheet_name='773TablesCount')
#     df_queries_datatypes = pandas.read_excel(config_excel['Body'], sheet_name='773TablesDatatypes')
# except Exception as e:
#     print("Failed to read data from excel sheet")
#     print(str(e))
#     print("Exiting program")
#     sys.exit(-1)

# Athena params
params = {
    'bucket': 'test-bucket-python',
    'path': 'output',
    'query': 'SELECT * FROM sampledb.homes;'
}

# Athena start query execution
response = client_athena.start_query_execution(
    QueryString=params['query'],
    ResultConfiguration={
        'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
    }
)

execution_id = response['QueryExecutionId']

# Execution time in seconds
max_execution = 60

# Result file from S3 output location
filename = ''
error = ''

state = 'RUNNING'
while max_execution > 0 and state in ['RUNNING', 'QUEUED']:
    max_execution = max_execution - 1
    response = client_athena.get_query_execution(QueryExecutionId=execution_id)
    # print(response)

    if 'QueryExecution' in response and \
            'Status' in response['QueryExecution'] and \
            'State' in response['QueryExecution']['Status']:
        state = response['QueryExecution']['Status']['State']
        if state == 'FAILED':
            error = response['QueryExecution']['Status']['StateChangeReason']
            break
        elif state == 'SUCCEEDED':
            filename = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            break
    time.sleep(1)

if filename != '':
    # Get bucket and key from output filename
    bucket = filename.replace('s3://', '').split('/', 1)[0]
    key = filename.replace('s3://', '').split('/', 1)[1]

    # Read result from S3 into pandas
    obj_df = client_s3.get_object(Bucket=bucket, Key=key)
    df = pandas.read_csv(obj_df['Body'])
else:
    print(error)
