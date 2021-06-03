# Read data from csv in S3 using Athena and store into Spark dataframe

import findspark
findspark.init()

import boto3
import time
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext

# Default profile
client_athena = boto3.client('athena')

# Athena params
params = {
    'region': 'ap-south-1',
    'database': 'homes',
    'bucket': 'test-bucket-python',
    'path': 'output',
    'query': 'SELECT * FROM "AwsDataCatalog"."sampledb"."homes";'
}

# Athena start query execution
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
            filename = filename.replace("s3:", "s3a:")
            break
    time.sleep(1)

print(filename)

# filename = "s3a://test-bucket-python/output/64dc1b3c-8c1c-489b-bd89-eb8c886d8968.csv"

# --------------------------------
# Configure spark to connect to S3
# --------------------------------
conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

# ---------------------------------------------------------------------
# For standard credentials (access key + secret key), use the following
# ---------------------------------------------------------------------
conf.set("spark.hadoop.fs.s3a.access.key", "access_key")
conf.set("spark.hadoop.fs.s3a.secret.key", "secret_key")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
# ---------------------------------------------------------------------

# --------------------------------------------
# For temporary credentials, use the following
# --------------------------------------------
# conf.set("spark.hadoop.fs.s3a.access.key", "access_key")
# conf.set("spark.hadoop.fs.s3a.secret.key", "secret_key")
# conf.set("spark.hadoop.fs.s3a.session.token", "session_token")
# conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
# --------------------------------------------

sc = SparkContext(conf=conf)
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

spark = SparkSession.builder.appName("AWS_Spark").config(conf=conf).getOrCreate()
# --------------------------------

# ----------------------------------------------------------------------
# NOTE: Spark configurations for S3 can also be set as below:
# ----------------------------------------------------------------------
# spark = SparkSession.builder \
#     .appName("AWS_Spark") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.4") \
#     .getOrCreate()
#
# # Get spark context
# sc = spark.sparkContext
#
# # Set configurations using spark context
# sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "access_key")
# sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "secret_key")
# sc._jsc.hadoopConfiguration() \
#     .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
# sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
# ----------------------------------------------------------------------
# Note that, when using <context>._jsc.hadoopConfiguration() as above,
# "spark.hadoop." prefix is not provided in property name
# ----------------------------------------------------------------------

if filename != '':
    sparkdf = spark.read.options(header='true', inferSchema='true').csv(filename)
    sparkdf.show(sparkdf.count())
    print("Number of records: {}".format(sparkdf.count()))
else:
    print(error)

spark.stop()
