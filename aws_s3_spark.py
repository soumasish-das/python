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
    'database': 'sampledb',
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

# --------------------------------
# Configure spark to connect to S3
# --------------------------------
conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

# --------------------------------------------------------------
# For profile credentials from .aws directory, use the following
# --------------------------------------------------------------
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
# --------------------------------------------------------------

# ---------------------------------------------------------------------
# For standard credentials (access key + secret key), use the following
# ---------------------------------------------------------------------
# conf.set("spark.hadoop.fs.s3a.access.key", "AKIAZCEXFQX2OC6IG272")
# conf.set("spark.hadoop.fs.s3a.secret.key", "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")
# conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
# ---------------------------------------------------------------------

# --------------------------------------------
# For temporary credentials, use the following
# --------------------------------------------
# conf.set("spark.hadoop.fs.s3a.access.key", "ASIAZCEXFQX2D37RH4DK")
# conf.set("spark.hadoop.fs.s3a.secret.key", "rnG30hjoVf0jjYQrdncxYX988nqj2n82yhV+dEFq")
# conf.set("spark.hadoop.fs.s3a.session.token", "IQoJb3JpZ2luX2VjEEwaCmFwLXNvdXRoLTEiRzBFAiByw1nAZyF0uWT+QaQtXTas11HEamtwIsbKOUt6xo+7bgIhAKGXp3Slj8MJv6ryGADTvJ0xNwk5QGVzwcvgQGdaAITJKvMBCPX//////////wEQABoMNjIzMDg3Mjg5ODQ0Igz53VH+m9kz9SvH4z8qxwFLnQG9chPQ93lO7VIFs4kqC96SRNK7ofUELkewHCKXnkqSe/Cv6sR5A7nHP55od6syrfqlZFajMk3vIqHO53koHm9ergj/0nufx/jSYo7HoKzElEsMz3ke6vzfgibpc/qfwlrlnMgBTdzFkB0YIHFiOVOqrPKQrllPlwXx51UKLHQnfmLHkroehylxkLqhijt2fzbSsvHhImy47AI67ZL+IzqxSWU9w7MfvLTztoZ8A4764+9rJXrE+0K+RCye69+wP0wx+Sq+MLXt5IUGOpgB+UnClnJUb9+YsR7zd0+jjE32ciDcBFqwkVewRp6svrS7mjVY84PDoglk9C+qyI8LOnGd0X7l9rFndUze3F+/ZcPRvkkTMfu1HRqn6+2A0450cmv+Udp2WIYePuu777qEr67KukY4J5zsP6MU4nicL9odICccHxnBJhz0OvpcTTxTa+f87TMKIrrag3eWnwrZf4gYvs1dFrk=")
# conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
# --------------------------------------------

# --------------------------
# Set other Spark parameters
# --------------------------
# conf.set("spark.master", "spark://192.168.0.103:7077")
conf.setAppName("AWS_Spark")
# --------------------------

sc = SparkContext(conf=conf)
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc.setLogLevel("ERROR")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")
# --------------------------------

# ----------------------------------------------------------------------
# NOTE: Spark configurations for S3 can also be set as below:
# ----------------------------------------------------------------------
# spark = SparkSession.builder \
#     .appName("AWS_Spark") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
#     .getOrCreate()
#
# # Get spark context
# sc = spark.sparkContext
#
# # Set configurations using spark context
# sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZCEXFQX2OC6IG272")
# sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
# sc._jsc.hadoopConfiguration() \
#     .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
# sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
# ----------------------------------------------------------------------
# Note that, when using <context>._jsc.hadoopConfiguration() as above,
# "spark.hadoop." prefix is not necessary in property name
# ----------------------------------------------------------------------

if filename != '':
    sparkdf = spark.read.options(header='true', inferSchema='true').csv(filename)
    sparkdf.show(sparkdf.count(), truncate=False)
    print("Number of records: {}".format(sparkdf.count()))
else:
    print(error)

spark.stop()

# Cleaning up Athena result files in S3
client_s3 = boto3.client('s3')
bucket = filename.replace('s3a://', '').split('/', 1)[0]
key = filename.replace('s3a://', '').split('/', 1)[1]
client_s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key}, {'Key': key+".metadata"}]})
print("\nAthena output files removed from S3.")
