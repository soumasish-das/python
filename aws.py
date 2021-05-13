#Read data from csv in S3 using Athena

import boto3
import pandas
import time

#Default profile
client_athena = boto3.client('athena')
client_s3 = boto3.client('s3')

#Athena params
params = {
    'region': 'ap-south-1',
    'database': 'sampledb',
    'bucket': 'test-bucket-python',
    'path': 'output',
    'query': 'SELECT * FROM "AwsDataCatalog"."sampledb"."homes";'
}

#Athena start query execution
response = client_athena.start_query_execution(
        QueryString=params['query'],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )

execution_id = response['QueryExecutionId']

#Execution time in seconds
max_execution = 60

#Result file from S3 output location
filename = ''

state = 'RUNNING'
while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        response = client_athena.get_query_execution(QueryExecutionId = execution_id)
        #print(response)

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
    #Get bucket and key from output filename
    bucket = filename.replace('s3://','').split('/', 1)[0]
    key = filename.replace('s3://','').split('/', 1)[-1]

    #Read result from S3 into pandas and print
    obj_df = client_s3.get_object(Bucket=bucket, Key=key)
    df = pandas.read_csv(obj_df['Body'])
    print(df.to_string(index=False))
else:
    print(error)
