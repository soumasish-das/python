import findspark
findspark.init()

from pyspark.sql import SparkSession
from spark_jdbc_connections import *

spark = SparkSession.builder \
    .appName("Spark-JDBC") \
    .config("spark.jars", "D:\\Softwares\\MySQL\\Connector J 8.0\\mysql-connector-java-8.0.27.jar,"
                          "D:\\Softwares\\Oracle\\ojdbc8.jar,"
                          "D:\\Softwares\\Microsoft SQL Server\\sqljdbc_9.4\\enu\\mssql-jdbc-9.4.0.jre8.jar,"
                          "D:\\Softwares\\PostgreSQL\\pgJDBC\\postgresql-42.3.0.jar,"
                          "D:\\Softwares\\SQLite\\sqlite-jdbc-3.36.0.1.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read data from MySQL table
mysqldict = {"url": "localhost:3306/world",
             "user": "mysql_user",
             "password": "test",
             "query": "select * from country"}
mysqlDF = getMySqlDF(spark, mysqldict)
print("----------------------------")
print("MySQL database sample table:")
print("----------------------------\n")
print("COUNTRY:")
mysqlDF.show(mysqlDF.count(), truncate=False)
print("Number of rows: {}".format(mysqlDF.count()))

print("\n")

# Read data from Oracle table
oracledict = {"url": "localhost:1521:orcl",
              "user": "hr",
              "password": "hr",
              "query": "select * from employees"}
oracleDF = getOracleDF(spark, oracledict)
print("-----------------------------")
print("Oracle database sample table:")
print("-----------------------------\n")
print("EMPLOYEES:")
oracleDF.show(oracleDF.count(), truncate=False)
print("Number of rows: {}".format(oracleDF.count()))

print("\n")

# Read data from SQL Server table
# jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks
# For Windows domain credentials logon:
# jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;integratedSecurity=true
sqlserverdict = {"url": "VICKY-PC\\SQLEXPRESS;databaseName=AdventureWorks",
                 "user": "sql_server_user",
                 "password": "test_ss",
                 "query": "select * from Production.Product"}
sqlServerDF = getSqlServerDF(spark, sqlserverdict)
print("---------------------------------")
print("SQL Server database sample table:")
print("---------------------------------\n")
print("PRODUCT:")
sqlServerDF.show(sqlServerDF.count(), truncate=False)
print("Number of rows: {}".format(sqlServerDF.count()))

print("\n")

# Read data from PostgreSQL table
pgdict = {"url": "localhost:5432/dvdrental",
          "user": "test_pg",
          "password": "postgre_test",
          "query": "select * from public.address"}
pgDF = getPgGpDF(spark, pgdict)
print("---------------------------------")
print("PostgreSQL database sample table:")
print("---------------------------------\n")
print("ADDRESS:")
pgDF.show(pgDF.count(), truncate=False)
print("Number of rows: {}".format(pgDF.count()))

print("\n")

# Read data from SQLite table
sqlitedict = {"url": "D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Chinook.db",
              "query": "select * from Customer"}
sqLiteDF = getSqLiteDF(spark, sqlitedict)
print("-----------------------------")
print("SQLite database sample table:")
print("-----------------------------\n")
print("CUSTOMER:")
sqLiteDF.show(sqLiteDF.count(), truncate=False)
print("Number of rows: {}".format(sqLiteDF.count()))

spark.stop()
