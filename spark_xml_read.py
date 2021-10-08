# Read XML into Spark using Databricks Spark-XML API
# https://github.com/databricks/spark-xml

import findspark
findspark.init()

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType


# ----------------------------------------------------
# Recursive function to flatten nested elements in XML
# https://stackoverflow.com/a/50156142
# ----------------------------------------------------
def get_flattened_fields(schema, prefix):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += get_flattened_fields(dtype, prefix=name)
        else:
            fields.append(name)
    return fields


def flatten(dataframe, prefix=None):
    schema = dataframe.schema
    dataframe = dataframe.select(get_flattened_fields(schema, prefix))
    dataframe = dataframe.toDF(*(c.replace('____', '') for c in dataframe.columns))
    return dataframe
# ----------------------------------------------------

# ---------------------------------------------------------------------------
# Create temp XML file to remove the problem-causing attribute xsi:nil="true"
# ---------------------------------------------------------------------------
def createTempFile(filepath, output_dir):
    with open(filepath, 'r') as file:
        filedata = file.read()

    filedata = filedata.replace('xsi:nil="true"', '')

    with open(output_dir + 'temp.xml', 'w') as file:
        file.write(filedata)

    return output_dir + 'temp.xml'
# ---------------------------------------------------------------------------


spark = SparkSession.builder \
    .appName("Spark-XML") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("xml_data1.xml:")
path = createTempFile('C:\\Users\\Vicky\\Minnie\\XML data\\xml_data1.xml', 'C:\\Users\\Vicky\\Desktop\\')
sparkDF = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "business_key") \
    .option("attributePrefix", "____") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("samplingRatio", 0.1) \
    .load(path)
# sparkDF.printSchema()
sparkDF = flatten(sparkDF)
sparkDF.show(truncate=False)

print("xml_data2.xml:")
path = createTempFile('C:\\Users\\Vicky\\Minnie\\XML data\\xml_data2.xml', 'C:\\Users\\Vicky\\Desktop\\')
sparkDF = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "business_key") \
    .option("attributePrefix", "____") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("samplingRatio", 0.1) \
    .load(path)
# sparkDF.printSchema()
sparkDF = flatten(sparkDF)
sparkDF.show(truncate=False)

print("xml_data3.xml:")
path = createTempFile('C:\\Users\\Vicky\\Minnie\\XML data\\xml_data3.xml', 'C:\\Users\\Vicky\\Desktop\\')
sparkDF = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "business_key") \
    .option("attributePrefix", "____") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("samplingRatio", 0.1) \
    .load(path)
# sparkDF.printSchema()
sparkDF = flatten(sparkDF)
sparkDF.show(truncate=False)

sparkDF.createOrReplaceTempView("PERSON")
df = spark.sql("SELECT id, name, gender, dob, description, city, company, salary FROM PERSON")
print("\nPERSON:")
df.show(truncate=False)

print("\nCOLUMNS:")
columns = spark.createDataFrame(sparkDF.columns, StringType())
columns.createOrReplaceTempView("COLUMNS")
columns = spark.sql("select upper(value) as column from COLUMNS")
columns.show(truncate=False)

print('Get "columns" dataframe values as a list:')
list = [r[0] for r in columns.select('*').toLocalIterator()]
print(list)

print("\nNEW COLUMNS WITH NULL VALUE:")
new_column_list = ['age', 'address']
sql = 'select *'
for column in new_column_list:
    sql += ', null as ' + column
sql += ' from Person'
print(sql)
sparkDF = spark.sql(sql)
sparkDF.show(truncate=False)

spark.catalog.dropTempView("Person")
spark.catalog.dropTempView("Columns")

os.remove(path)

spark.stop()
