# Read XML into Spark using Databricks Spark-XML API
# https://github.com/databricks/spark-xml

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("Spark-XML") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sparkDF = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "person") \
        .option("excludeAttribute", "true") \
        .option("treatEmptyValuesAsNulls", "true") \
        .load("C:\\Users\\Vicky\\Minnie\\XML data\\*.xml")

sparkDF.printSchema()
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

spark.stop()
