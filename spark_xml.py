# Read XML into Spark using Databricks Spark-XML API
# https://github.com/databricks/spark-xml

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark-XML") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sparkDF = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "database") \
        .option("rowTag", "person") \
        .load("C:\\Users\\Vicky\\Minnie\\xml_data*.xml")

sparkDF.printSchema()
sparkDF.show(truncate=False)

sparkDF.createOrReplaceTempView("PERSON")
print("\nPERSON:")
spark.sql("SELECT id, name, gender, dob, city, company, salary FROM PERSON").show(truncate=False)
spark.catalog.dropTempView("Person")

spark.stop()
