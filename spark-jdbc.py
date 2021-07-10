import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark-JDBC") \
    .config("spark.jars", "D:\\Softwares\\MySQL\\Connector J 8.0\\mysql-connector-java-8.0.25.jar,"
                          "D:\\Softwares\\Oracle\\ojdbc8.jar,"
                          "D:\\Softwares\\Microsoft SQL Server\\sqljdbc_9.2\\enu\\mssql-jdbc-9.2.1.jre8.jar,"
                          "D:\\Softwares\\SQLite\\sqlite-jdbc-3.36.0.1.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read data from MySQL table
mysqlDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/world") \
    .option("user", "mysql_user") \
    .option("password", "test") \
    .option("query", "select * from country") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()
print("----------------------------")
print("MySQL database sample table:")
print("----------------------------\n")
print("COUNTRY:")
mysqlDF.show(mysqlDF.count(), truncate=False)
print("Number of rows: {}".format(mysqlDF.count()))

print("\n")

# Read data from Oracle table
oracleDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@localhost:1521:orcl") \
    .option("user", "hr") \
    .option("password", "hr") \
    .option("query", "select * from employees") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()
print("-----------------------------")
print("Oracle database sample table:")
print("-----------------------------\n")
print("EMPLOYEES:")
oracleDF.show(oracleDF.count(), truncate=False)
print("Number of rows: {}".format(oracleDF.count()))

print("\n")

# Read data from SQL Server table
# jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks
sqlServerDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://VICKY-PC\\SQLEXPRESS;databaseName=AdventureWorks") \
    .option("user", "sql_server_user") \
    .option("password", "test_ss") \
    .option("query", "select * from Production.Product") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
print("---------------------------------")
print("SQL Server database sample table:")
print("---------------------------------\n")
print("PRODUCT:")
sqlServerDF.show(sqlServerDF.count(), truncate=False)
print("Number of rows: {}".format(sqlServerDF.count()))

print("\n")

# Read data from SQLite table
sqLiteDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Chinook.db") \
    .option("query", "select * from Customer") \
    .option("driver", "org.sqlite.JDBC") \
    .load()
print("---------------------------------")
print("SQLite database sample table:")
print("---------------------------------\n")
print("CUSTOMER:")
sqLiteDF.show(sqLiteDF.count(), truncate=False)
print("Number of rows: {}".format(sqLiteDF.count()))

spark.stop()
