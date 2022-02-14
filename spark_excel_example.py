# Read and write with Spark dataframe directly from/to excel file using spark-excel library

# ----------------------------
# spark-excel library source:
# ----------------------------
# https://github.com/crealytics/spark-excel

# --------------------------------------
# spark-excel version and dependencies:
# --------------------------------------
# Maven Repository:
# https://mvnrepository.com/artifact/com.crealytics/spark-excel_2.13/3.2.1_0.16.4

# spark-excel information from the Maven repository page:
# --------------------------------------------------------
# groupId: com.crealytics
# artifactId: spark-excel_2.13
# version: 3.2.1_0.16.4

# Format: spark.jars.packages  groupId:artifactId:version

# ---------------------------
# INSTALLATION INSTRUCTIONS:
# ---------------------------
# 1. Open "spark-defaults.conf" in "%SPARK_HOME%\conf" folder
# 2. Add "spark.jars.packages  com.crealytics:spark-excel_2.13:3.2.1_0.16.4"

# --------------------------------------------------------
# Reference for read/write excel from/to spark dataframe:
# --------------------------------------------------------
# https://github.com/crealytics/spark-excel/blob/main/README.md


import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadExcel") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.13:3.2.1_0.16.4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# -----------------------------------------
# Get Spark UI page URL from Spark Context
# -----------------------------------------
# sc = spark.sparkContext
# print(sc.uiWebUrl)
# -----------------------------------------

# Read from excel file
sparkDF = spark.read.format("com.crealytics.spark.excel") \
    .options(header='true', inferSchema='true', dataAddress="'Data'!A1", usePlainNumberFormat='true') \
    .load("C:\\Users\\Vicky\\Minnie\\SampleXLSFile.xls")

print("Data read from excel file successfully.\n")

sparkDF.printSchema()

sparkDF.createOrReplaceTempView("excel_data")
spark.sql("select count(*) from excel_data").show()

excelWriteDF = spark.sql("select * from excel_data where UserId < 10000")
print("excelWriteDF row count: " + str(excelWriteDF.count()))

# Write into excel file
excelWriteDF.write.format("com.crealytics.spark.excel") \
    .options(header='true', dataAddress="'Result'!A1", usePlainNumberFormat='true') \
    .mode("overwrite") \
    .save("C:\\Users\\Vicky\\Minnie\\spark-excel-write-test.xlsx")

print("\nData written to excel file successfully.")

spark.stop()
