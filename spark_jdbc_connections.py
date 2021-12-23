# Spark JDBC connection definitions

# -------------------
# Usage instructions:
# -------------------
# Use the respective method to get data as a Spark dataframe from a particular database.
#
# Parameters:
# -----------
# spark_obj:       Spark variable that contains Spark initialization information
# connection_dict: Dictionary of database connection options
#
# Example usage for MySQL database:
# ---------------------------------
# from pyspark.sql import SparkSession
# from spark_jdbc_connections import getMySqlDF
# spark = SparkSession.builder \
#         .appName("Spark-JDBC") \
#         .config("spark.jars", "D:\\Softwares\\MySQL\\Connector J 8.0\\mysql-connector-java-8.0.27.jar") \
#         .getOrCreate()
# mysqldict = {"url": "localhost:3306/world",
#              "user": "mysql_user",
#              "password": "test",
#              "query": "select * from country"}
# mysqlDF = getMySqlDF(spark, mysqldict)
# mysqlDF.show(mysqlDF.count(), truncate=False)
# spark.stop()
#
# In this example, getMySqlDF() function is used to get a Spark dataframe containing
# data queried from MySQL database.
# Note that the dictionary contains all the parameters that are normally passed as
# options to connect to the database using JDBC (and Spark in this case).
#
# These helper functions add the JDBC specifications for the respective database in
# the connection URL and also provides the JDBC driver class names to Spark. This
# ensures that the programmer does not have to remember the driver class names or
# the JDBC connection URL specifications pertaining to their respective databases.
# -------------------


# Generic function to set connection options (like url, user, password etc.) and get data in Spark dataframe
def getData(spark_df, connection_dict):
    for key in connection_dict:
        spark_df = spark_df.option(key, connection_dict[key])
    return spark_df.load()


# Get SQLite data
def getSqLiteDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:sqlite://" + connection_dict['url']
    sqLiteDF = spark_obj.read.format("jdbc").option("driver", "org.sqlite.JDBC")
    return getData(sqLiteDF, connection_dict)


# Get PostgreSQL/GreenPlum data
def getPgGpDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:postgresql://" + connection_dict['url']
    pgGpDF = spark_obj.read.format("jdbc").option("driver", "org.postgresql.Driver")
    return getData(pgGpDF, connection_dict)


# Get Oracle data
def getOracleDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:oracle:thin:@" + connection_dict['url']
    oracleDF = spark_obj.read.format("jdbc").option("driver", "oracle.jdbc.driver.OracleDriver")
    return getData(oracleDF, connection_dict)


# Get SQL Server data
def getSqlServerDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:sqlserver://" + connection_dict['url']
    sqlServerDF = spark_obj.read.format("jdbc").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    return getData(sqlServerDF, connection_dict)


# Get MySQL data
def getMySqlDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:mysql://" + connection_dict['url']
    mySqlDF = spark_obj.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver")
    return getData(mySqlDF, connection_dict)


# Get DB2 data
def getDB2DF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:db2://" + connection_dict['url']
    db2DF = spark_obj.read.format("jdbc").option("driver", "com.ibm.db2.jcc.DB2Driver")
    return getData(db2DF, connection_dict)


# Get Redshift data
def getRedshiftDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:redshift://" + connection_dict['url']
    redshiftDF = spark_obj.read.format("jdbc").option("driver", "com.amazon.redshift.jdbc42.Driver")
    return getData(redshiftDF, connection_dict)


# Get Snowflake data
def getSnowflakeDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:snowflake://" + connection_dict['url']
    snowflakeDF = spark_obj.read.format("jdbc").option("driver", "net.snowflake.client.jdbc.SnowflakeDriver")
    return getData(snowflakeDF, connection_dict)


# Get Sybase data
def getSybaseDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:sybase:Tds:" + connection_dict['url']
    sybaseDF = spark_obj.read.format("jdbc").option("driver", "com.sybase.jdbc42.jdbc.SybDriver")
    return getData(sybaseDF, connection_dict)


# Get Teradata data
def getTeradataDF(spark_obj, connection_dict):
    connection_dict['url'] = "jdbc:teradata://" + connection_dict['url']
    teradataDF = spark_obj.read.format("jdbc").option("driver", "com.teradata.jdbc.TeraDriver")
    return getData(teradataDF, connection_dict)

