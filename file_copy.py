import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, when, count, col, isnan

from shutil import copyfile
import os
import random
import glob

mylist = [f for f in glob.glob("C:\\Users\\Vicky\\Desktop\\*.txt")]
print(mylist)
print("\n")

output_dir = "C:\\Users\\Vicky\\Minnie\\"
dest_list = []

for file in mylist:
    #Generate name of copied file using random numbers
    #random.randint(100,1000) gives a number between 100 and 1000
    dest = output_dir + os.path.basename(file) + "_" + str(random.randint(100,1000))
    dest_list.append(dest)
    
    try:
        #Copy file
        copyfile(file, dest)
    except Exception as e:
        print("Exception: " + str(e))


spark = SparkSession.builder.appName("Test").getOrCreate()

sparkdf = spark.read.options(header='true', inferSchema='true', delimiter='\t')\
                    .csv("C:\\Users\\Vicky\\Minnie\\*.am1*")
sparkdf = sparkdf.select([when(isnan(c) | isnull(c), None).otherwise(col(c)).alias(c) for c in sparkdf.columns])

sparkdf.createOrReplaceTempView("test")
spark.sql("SELECT count(*) FROM test").show()

spar.stop()

#Delete the copied file if it exists
for file in dest_list:
   if os.path.isfile(file):
      os.remove(file)
