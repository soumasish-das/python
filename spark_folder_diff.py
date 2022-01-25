import findspark
findspark.init()

import os
import subprocess
import shutil
from pyspark.sql import SparkSession


# Remove the end part of filenames in folder from the last "_" until file extension
def fileRename(fileList, folderPath):
    for i in range(len(fileList)):
        file_name = os.path.splitext(fileList[i])[0]
        file_ext = os.path.splitext(fileList[i])[1]
        lastunderscore = file_name.rfind('_')
        if lastunderscore != -1:
            new_file_name = file_name[:lastunderscore] + file_ext
            os.rename(folderPath + fileList[i], folderPath + new_file_name)
            fileList[i] = new_file_name
    return fileList


src_folder = "C:\\Users\\Vicky\\Minnie\\TEST_SRC"
tgt_folder = "C:\\Users\\Vicky\\Minnie\\TEST_TGT"

result_folder = "C:\\Users\\Vicky\\Minnie\\Result\\"
src_result_folder = result_folder + "SRC\\"
tgt_result_folder = result_folder + "TGT\\"

# Copy source folder
shutil.copytree(src_folder, src_result_folder)
source_content = str(subprocess.check_output("dir /B /A-D " + src_result_folder, shell=True).strip(), 'utf-8')
source_content = source_content.split("\r\n")
# Rename files
source_content = fileRename(source_content, src_result_folder)
print("Copied source folder to result directory successfully.")

# Copy target folder
shutil.copytree(tgt_folder, tgt_result_folder)
target_content = str(subprocess.check_output("dir /B /A-D " + tgt_result_folder, shell=True).strip(), 'utf-8')
target_content = target_content.split("\r\n")
# Rename files
target_content = fileRename(target_content, tgt_result_folder)
print("Copied target folder to result directory successfully.\n")

# We are assuming that source_content >= target_content
prima_list = source_content
sec_list = target_content

# If target_content >= source_content, swap prima_list and sec_list
if len(source_content) < len(target_content):
    prima_list = target_content
    sec_list = source_content

# Initialize Spark
spark = SparkSession.builder \
    .appName("Spark-Folder-Diff") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sql = "select * from source minus select * from target " \
      "union " \
      "select * from target minus select * from source"

try:
    for item in prima_list:
        if item in sec_list:
            sparkDF_source = spark.read.options(header='true', inferSchema='true').csv(src_result_folder + item)
            sparkDF_target = spark.read.options(header='true', inferSchema='true').csv(tgt_result_folder + item)
            sparkDF_source.createOrReplaceTempView("source")
            sparkDF_target.createOrReplaceTempView("target")
            spark.sql(sql).show(truncate=False)
except Exception as e:
    print("Exception: \n" + str(e))
finally:
    if os.path.exists(src_result_folder):
        shutil.rmtree(src_result_folder)
        print("Removed source folder from result directory successfully.")
    if os.path.exists(tgt_result_folder):
        shutil.rmtree(tgt_result_folder)
        print("Removed target folder from result directory successfully.")

spark.stop()
print("\nProgram completed successfully.")
