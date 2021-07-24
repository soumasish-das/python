# Write into DB using Spark

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder \
    .appName("spark-write-to-DB") \
    .config("spark.jars", "D:\\Softwares\\SQLite\\sqlite-jdbc-3.36.0.1.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# "SOURCE" table
df_source = spark.read.options(header='true', inferSchema='true') \
    .csv("C:\\Users\\Vicky\\Minnie\\5m Sales Records - original.csv")
df_source.createOrReplaceTempView("SOURCE")

# "TARGET" table
df_target = spark.read.options(header='true', inferSchema='true') \
    .csv("C:\\Users\\Vicky\\Minnie\\5m Sales Records - modified.csv")

# Write SOURCE data into SQLite table
# .option("numPartitions", "1") is for non-concurrent write for use in SQLite only
# Do not use for other DBs
df_source.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:sqlite:D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Sample.db") \
    .option("dbtable", "SOURCE") \
    .option("numPartitions", "1") \
    .option("driver", "org.sqlite.JDBC") \
    .save()
print("SOURCE written to DB")

# Write TARGET data into SQLite table
# .option("numPartitions", "1") is for non-concurrent write for use in SQLite only
# Do not use for other DBs
df_target.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:sqlite:D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Sample.db") \
    .option("dbtable", "TARGET") \
    .option("numPartitions", "1") \
    .option("driver", "org.sqlite.JDBC") \
    .save()
print("TARGET written to DB")

spark.stop()
