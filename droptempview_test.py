import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

sparkdf = spark.read.options(header='true', inferSchema='true').csv("C:\\Users\\Vicky\\Minnie\\test2.csv")

sparkdf.createOrReplaceTempView("test1")
spark.sql("select * from test1").show(truncate=False)

sparkdf.createOrReplaceTempView("test2")
sparkdf.createOrReplaceTempView("test3")
sparkdf.createOrReplaceTempView("test4")

tempviewname_list = ['test1', 'test2', 'test3', 'test4']

spark.catalog.dropTempView("test1")
try:
    spark.sql("select * from test1").show(truncate=False)
except Exception as e:
    print("------\nError:\n------\n" + str(e) + "------")
finally:
    for viewname in tempviewname_list:
        spark.catalog.dropTempView(viewname)
    print("\nDone")

spark.stop()
