# Spark Diff Tool

import findspark
findspark.init()

from pyspark.sql import SparkSession

import sys
import os
import glob
import shutil
import time
import argparse
import logging


# -----------------------------
# Set up command line arguments
# -----------------------------
parser = argparse.ArgumentParser(description='Spark Diff Tool: A diff tool that compares 2 datasets using Spark. '
                                             'Use this to find differences in column values, missing records and '
                                             'duplicate records amongst the datasets.',
                                 fromfile_prefix_chars='@',
                                 allow_abbrev=False)

# Required arguments
required_args = parser.add_argument_group('required arguments')
required_args.add_argument("-k", "--keys",
                           help="Comma separated list of key columns to be used in the diff process",
                           required=True)
required_args.add_argument("-o", "--output-dir",
                           help="Output directory to generate diff result and log",
                           required=True)
required_args.add_argument("-s", "--source",
                           help="Path of the source file to compare",
                           required=True)
required_args.add_argument("-t", "--target",
                           help="Path of the target file to compare",
                           required=True)
required_args.add_argument("-d", "--delimiter",
                           help="Delimiter used in the input files to separate data into columns",
                           required=True)

# Optional arguments
parser.add_argument("-m", "--ignore-missing",
                    help="Use this option to exclude missing records in the diff result",
                    action='store_true',
                    default=False)
parser.add_argument("-D", "--ignore-duplicates",
                    help="Use this option to exclude duplicate records count in the diff result",
                    action='store_true',
                    default=False)
parser.add_argument("-x", "--exclude-columns",
                    help="Comma separated list of columns to be excluded "
                         "from source and target datasets for comparison")
parser.add_argument("-M", "--spark-master",
                    help="Spark master address to run in cluster mode. If this is not specified, "
                         "then Spark runs in standalone mode")

args = parser.parse_args()
# -----------------------------

# For Windows only, to have the path in a clean manner
args.output_dir = args.output_dir.replace('\\\\', '\\')

LOG_FILE_FULL_PATH = os.path.join(args.output_dir, 'diff.log')
CONSOLE_ERROR_MESSAGE = "ERROR: Process failed. Please check {} for details.\n".format(LOG_FILE_FULL_PATH)
PROGRAM_EXIT_MESSAGE = "Stopping Spark and exiting program..."

logging.basicConfig(filename=LOG_FILE_FULL_PATH,
                    filemode='w',
                    format='%(asctime)s - [%(levelname)s] - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

try:
    if args.spark_master:
        spark = SparkSession.builder.master(args.spark_master).appName("Spark_Diff_Tool").getOrCreate()
    else:
        spark = SparkSession.builder.appName("Spark_Diff_Tool").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.info("Started Spark process")
except Exception:
    logging.critical("ERROR: Failed to start Spark. Stack trace:", exc_info=True)
    logging.critical("Exiting program...")
    print(CONSOLE_ERROR_MESSAGE)
    sys.exit(1)

# Begin timer
t_start = time.time()

# Read contents of source file into Spark dataframe
try:
    df_source = spark.read.options(header='true', inferSchema='true', sep=args.delimiter) \
        .csv(args.source)
except Exception:
    logging.critical("ERROR: Could not read SOURCE file. Stack trace:", exc_info=True)
    logging.critical(PROGRAM_EXIT_MESSAGE)
    spark.stop()
    print(CONSOLE_ERROR_MESSAGE)
    sys.exit(1)
logging.info("Read SOURCE file in {} seconds".format(time.time() - t_start))

# Read contents of target file into Spark dataframe
t0 = time.time()
try:
    df_target = spark.read.options(header='true', inferSchema='true', sep=args.delimiter) \
        .csv(args.target)
except Exception:
    logging.critical("ERROR: Could not read TARGET file. Stack trace:", exc_info=True)
    logging.critical(PROGRAM_EXIT_MESSAGE)
    spark.stop()
    print(CONSOLE_ERROR_MESSAGE)
    sys.exit(1)
logging.info("Read TARGET file in {} seconds".format(time.time() - t0))

diff_key = args.keys

# -------------------------------------------------
# Change column names of TARGET to that of SOURCE
# This is done to ensure ease of comparison of data
# Also, convert column names to uppercase
# -------------------------------------------------
col_list_uppercase = [x.upper() for x in df_source.columns]
df_source = df_source.toDF(*col_list_uppercase)
try:
    df_target = df_target.toDF(*col_list_uppercase)
except Exception:
    logging.critical("ERROR: Issue in TARGET dataset. Stack trace:", exc_info=True)
    logging.critical(PROGRAM_EXIT_MESSAGE)
    spark.stop()
    print(CONSOLE_ERROR_MESSAGE)
    sys.exit(1)
# -------------------------------------------------

if args.exclude_columns:
    # -----------------------------
    # Exclude columns from datasets
    # -----------------------------
    exclude_col_list = [x.upper() for x in args.exclude_columns.split(",")]
    df_source = df_source.drop(*exclude_col_list)
    df_target = df_target.drop(*exclude_col_list)

    col_list_uppercase = [x.upper() for x in df_source.columns]
    # -----------------------------

df_source.createOrReplaceTempView("SOURCE")
df_target.createOrReplaceTempView("TARGET")

# ----------------------------------
# Set up the diff SQL pre-requisites
# ----------------------------------
diff_key_sql_join_source = ""
diff_key_sql_join_target = ""

for item in diff_key.split(","):
    try:
        col_list_uppercase.remove(item.upper())
    except Exception:
        logging.critical("ERROR: Key \"{}\" is not one of the unexcluded datasource columns {}"
                         .format(item.upper(), df_source.columns))
        logging.critical(PROGRAM_EXIT_MESSAGE)
        spark.stop()
        print(CONSOLE_ERROR_MESSAGE)
        sys.exit(1)
    diff_key_sql_join_source += "coalesce(s.{},'')||':'||".format(item)
    diff_key_sql_join_target += "coalesce(t.{},'')||':'||".format(item)
diff_key_sql_join_source = diff_key_sql_join_source[:-7]
diff_key_sql_join_target = diff_key_sql_join_target[:-7]
# ----------------------------------

diff_key_col_name = "KEY[{}]".format(diff_key.replace(',', ':'))

# -------------------
# Create the diff SQL
# -------------------
sql = "SELECT {} AS `{}`,\n".format(diff_key_sql_join_source, diff_key_col_name)
for col in col_list_uppercase:
    sql += "s.{} AS {}_SOURCE,\nt.{} AS {}_TARGET,\n".format(col, col, col, col)
    if dict(df_source.dtypes)[col] == "string":
        sql += "CASE WHEN coalesce(s.{},'') = coalesce(t.{},'') ".format(col, col)
        sql += "THEN 'No Diff' ELSE 'Text Diff' END AS {}_DIFF,\n".format(col)
    else:
        sql += "coalesce(t.{},'') - coalesce(s.{},'') AS {}_DIFF,\n".format(col, col, col)
sql += "'Diffs present' AS DIFF_COMMENT\nFROM SOURCE s JOIN TARGET t\nON {} = ".format(diff_key_sql_join_source)
sql += "{}\nWHERE ".format(diff_key_sql_join_target)
for col in col_list_uppercase:
    sql += "coalesce(s.{},'') != coalesce(t.{},'')\nOR ".format(col, col)
sql = sql[:-4]
# -------------------

if not args.ignore_missing:
    # ---------------------------------------------
    # Records available in SOURCE but not in TARGET
    # ---------------------------------------------
    sql += "\nUNION\nSELECT {} AS `{}`,\n".format(diff_key_sql_join_source, diff_key_col_name)
    for col in col_list_uppercase:
        sql += "s.{} AS {}_SOURCE,\nNULL AS {}_TARGET,\n'',\n".format(col, col, col)
    sql += "'Missing in TARGET' AS DIFF_COMMENT\nFROM SOURCE s\nWHERE NOT EXISTS " \
           "(SELECT 1 FROM TARGET t WHERE {} = {})".format(diff_key_sql_join_source, diff_key_sql_join_target)
    # --------------------------------------------

    # ---------------------------------------------
    # Records available in TARGET but not in SOURCE
    # ---------------------------------------------
    sql += "\nUNION\nSELECT {} AS `{}`,\n".format(diff_key_sql_join_target, diff_key_col_name)
    for col in col_list_uppercase:
        sql += "NULL AS {}_SOURCE,\nt.{} AS {}_TARGET,\n'',\n".format(col, col, col)
    sql += "'Missing in SOURCE' AS DIFF_COMMENT\nFROM TARGET t\nWHERE NOT EXISTS " \
           "(SELECT 1 FROM SOURCE s WHERE {} = {})".format(diff_key_sql_join_source, diff_key_sql_join_target)
    # --------------------------------------------

if not args.ignore_duplicates:
    # ------------------------
    # Duplicate SOURCE records
    # ------------------------
    sql += "\nUNION\nSELECT {} AS `{}`,\n".format(diff_key_sql_join_source, diff_key_col_name)
    for col in col_list_uppercase:
        sql += "'',\n'',\n'',\n"
    sql += "(COUNT({})-1)||' duplicate row(s) in SOURCE' AS DIFF_COMMENT\n".format(diff_key_sql_join_source)
    sql += "FROM SOURCE s\nGROUP BY 1\nHAVING COUNT({}) > 1".format(diff_key_sql_join_source)
    # ------------------------

    # ------------------------
    # Duplicate TARGET records
    # ------------------------
    sql += "\nUNION\nSELECT {} AS `{}`,\n".format(diff_key_sql_join_target, diff_key_col_name)
    for col in col_list_uppercase:
        sql += "'',\n'',\n'',\n"
    sql += "(COUNT({})-1)||' duplicate row(s) in TARGET' AS DIFF_COMMENT\n".format(diff_key_sql_join_target)
    sql += "FROM TARGET t\nGROUP BY 1\nHAVING COUNT({}) > 1".format(diff_key_sql_join_target)
    # ------------------------

logging.info("SQL used in Spark:")
logging.info(sql)

# DIFF dataframe
t0 = time.time()
try:
    df_diff = spark.sql(sql)

    logging.info("Performing diff and writing results to CSV file...")
    # Write to CSV file
    df_diff.coalesce(1) \
        .write.options(header='true', sep=',') \
        .csv(os.path.join(args.output_dir, 'diff'))

    # Move CSV file generated by spark to output directory and rename as diff.csv
    shutil.move(glob.glob(os.path.join(args.output_dir, 'diff', '*.csv'))[0], os.path.join(args.output_dir, 'diff.csv'))
    shutil.rmtree(os.path.join(args.output_dir, 'diff'))

    logging.info("Diff completed and result file generated in {} seconds".format(time.time() - t0))
    logging.info("Full diff process completed in {} seconds".format(time.time() - t_start))
except Exception:
    logging.critical("ERROR: Diff process failed. Stack trace:", exc_info=True)
    logging.critical(PROGRAM_EXIT_MESSAGE)
    spark.stop()
    print(CONSOLE_ERROR_MESSAGE)
    sys.exit(1)

spark.catalog.dropTempView("SOURCE")
spark.catalog.dropTempView("TARGET")
logging.info("Dropped SOURCE and TARGET temporary views")

spark.stop()
logging.info("Stopped Spark process")
