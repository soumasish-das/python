import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Get Spark Configs") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7") \
    .config("spark.jars", "D:\\Softwares\\MySQL\\Connector J 8.0\\mysql-connector-java-8.0.26.jar,"
                          "D:\\Softwares\\Oracle\\ojdbc8.jar,"
                          "D:\\Softwares\\Microsoft SQL Server\\sqljdbc_9.2\\enu\\mssql-jdbc-9.2.1.jre8.jar,"
                          "D:\\Softwares\\SQLite\\sqlite-jdbc-3.36.0.1.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Get all configs
spark_config_full_list = spark.sparkContext.getConf().getAll()
for item in spark_config_full_list:
    print(item)
print()

# Get a specific config
print(spark.conf.get("spark.serializer"))
