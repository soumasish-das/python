import findspark
findspark.init()

from pyspark.sql import SparkSession

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

# Read data from Oracle table (OS Authentication used below; no username and password provided)
oracleDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@localhost:1521:orcl") \
    .option("query", "select * from hr.employees") \
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
# For Windows domain credentials logon:
# jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;integratedSecurity=true
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

# Read data from PostgreSQL table
pgDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/dvdrental") \
    .option("user", "test_pg") \
    .option("password", "postgre_test") \
    .option("query", "select * from public.address") \
    .option("driver", "org.postgresql.Driver") \
    .load()
print("---------------------------------")
print("PostgreSQL database sample table:")
print("---------------------------------\n")
print("ADDRESS:")
pgDF.show(pgDF.count(), truncate=False)
print("Number of rows: {}".format(pgDF.count()))

print("\n")

# Read data from SQLite table
sqLiteDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Chinook.db") \
    .option("query", "select * from Customer") \
    .option("driver", "org.sqlite.JDBC") \
    .load()
print("-----------------------------")
print("SQLite database sample table:")
print("-----------------------------\n")
print("CUSTOMER:")
sqLiteDF.show(sqLiteDF.count(), truncate=False)
print("Number of rows: {}".format(sqLiteDF.count()))

spark.stop()
