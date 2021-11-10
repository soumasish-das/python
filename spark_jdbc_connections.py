# Spark JDBC connection definitions

# Generic function to set connection options (like url, user, password etc.) and get data
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
