import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark-JDBC") \
    .config("spark.jars", "D:\\Softwares\\MySQL\\Connector J 8.0\\mysql-connector-java-8.0.25.jar,"
                          "D:\\Softwares\\Oracle\\ojdbc8.jar") \
    .getOrCreate()

# Read data from MySQL table
mysqlDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/world") \
    .option("user", "root") \
    .option("password", "root") \
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

spark.stop()
