# PySpark diff tool

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName("Diff_Tool").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Source dataframe
df_source = spark.createDataFrame \
        (
        [
            (100, "Steven King", "Steven@test.com", "516.123.4567"),
            (100, "Steven King", "Steven@test.com", "516.123.4567"),
            (100, "Steven King", "Steven@test.com", "516.123.4567"),
            (102, "Lex De Haan", "Lex@test.com", ""),
            (103, "Alexander Hunold", "", ""),
            (105, "David Austin", "", "590.423.4569"),
            (106, "Valli Pataballa", "Valli@test.com", ""),
            (107, "Diana Lorentz", "diana@test.com", "590.423.5567"),
            (108, "Bruce Wayne","Bruce@test.com", "999.888.777.6666"),
            (110, "Clark Kent","", "")
        ],
        ["EMP_ID", "EMP_NAME", "EMP_EMAIL", "EMP_PHONE"]
    )
df_source = df_source.select([when(col(c) == '', None).otherwise(col(c)).alias(c) for c in df_source.columns])

# Target dataframe
df_target = spark.createDataFrame \
        (
        [
            (100, "Steven King", "Steven@test.com", "515.123.4567"),
            (102, "Lex De Haan", "Lex@test.com", ""),
            (102, "Lex De Haan", "Lex@test.com", ""),
            (103, "Alexander Hunold", "", "590.423.4567"),
            (105, "DavidAustin", "", "590.423.4569"),
            (106, "Valli Pataballa", "ValliP@test.com", ""),
            (107, "Diana Lorentz", "Diana@test.com", "590.423.5567"),
            (109, "Scott Tiger", "scott@test.com", "590.423.4321")
        ],
        ["EMP_ID", "EMP_NAME", "EMP_EMAIL", "EMP_PHONE"]
    )
df_target = df_target.select([when(col(c) == '', None).otherwise(col(c)).alias(c) for c in df_target.columns])

print("SOURCE:")
df_source.show()

print("TARGET:")
df_target.show()

df_source.createOrReplaceTempView("SOURCE")
df_target.createOrReplaceTempView("TARGET")

diff_key = "EMP_ID,EMP_NAME"

col_list_without_key = df_source.columns
diff_key_sql_join_source = ""
diff_key_sql_join_target = ""
sql = ""

# ----------------------------------
# Set up the diff SQL pre-requisites
# ----------------------------------
# 1. Create the list of columns to be compared except the key columns for diff SQL.
#    Ex.: If ["EMP_ID", "EMP_NAME", "EMP_EMAIL", "EMP_PHONE"] is column set
#         and "EMP_ID,EMP_NAME" are key columns, then the columns to be compared
#         between SOURCE and TARGET are ["EMP_EMAIL", "EMP_PHONE"]
#
# 2. Set up the key column join condition for SOURCE and TARGET.
#    Ex.: coalesce(s.EMP_ID,'')||':'||coalesce(s.EMP_NAME,'') for SOURCE
#         coalesce(t.EMP_ID,'')||':'||coalesce(t.EMP_NAME,'') for TARGET
# ----------------------------------
for item in diff_key.split(","):
    col_list_without_key.remove(item)
    diff_key_sql_join_source += "coalesce(s." + item + ",'')||':'||"
    diff_key_sql_join_target += "coalesce(t." + item + ",'')||':'||"
diff_key_sql_join_source = diff_key_sql_join_source[:-7]
diff_key_sql_join_target = diff_key_sql_join_target[:-7]
# ----------------------------------

print("DIFF:")
# -------------------
# Create the diff SQL
# -------------------
# Example:
#
# SELECT coalesce(s.EMP_ID,'')||':'||coalesce(s.EMP_NAME,'') AS KEY, 'EMP_EMAIL' AS COLUMN, s.EMP_EMAIL AS SOURCE_VALUE, t.EMP_EMAIL AS TARGET_VALUE, '' AS DIFF_COMMENT
# FROM SOURCE s JOIN TARGET t
# ON coalesce(s.EMP_ID,'')||':'||coalesce(s.EMP_NAME,'') = coalesce(t.EMP_ID,'')||':'||coalesce(t.EMP_NAME,'')
# WHERE coalesce(s.EMP_EMAIL,'') != coalesce(t.EMP_EMAIL,'')
# UNION
# SELECT coalesce(s.EMP_ID,'')||':'||coalesce(s.EMP_NAME,'') AS KEY, 'EMP_PHONE' AS COLUMN, s.EMP_PHONE AS SOURCE_VALUE, t.EMP_PHONE AS TARGET_VALUE, '' AS DIFF_COMMENT
# FROM SOURCE s JOIN TARGET t
# ON coalesce(s.EMP_ID,'')||':'||coalesce(s.EMP_NAME,'') = coalesce(t.EMP_ID,'')||':'||coalesce(t.EMP_NAME,'')
# WHERE coalesce(s.EMP_PHONE,'') != coalesce(t.EMP_PHONE,'')
# -------------------
for col in col_list_without_key:
    sql += "SELECT " + diff_key_sql_join_source + " AS KEY, '" + col + "' AS COLUMN, s." + col + " AS SOURCE_VALUE, t."
    sql += col + " AS TARGET_VALUE, '' AS DIFF_COMMENT FROM SOURCE s JOIN TARGET t ON " + diff_key_sql_join_source
    sql += " = " + diff_key_sql_join_target + " WHERE coalesce(s." + col + ",'') != coalesce(t." + col + ",'')"
    sql += " UNION "
# -------------------

# ---------------------------------------------
# Records available in SOURCE but not in TARGET
# ---------------------------------------------
# Example:
#
# SELECT st.A AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE, 'Missing in TARGET' AS DIFF_COMMENT
# FROM (SELECT coalesce(s.EMP_ID,'')||':'||coalesce(s.EMP_NAME,'') AS A FROM SOURCE s MINUS SELECT coalesce(t.EMP_ID,'')||':'||coalesce(t.EMP_NAME,'') AS A FROM TARGET t) st
# --------------------------------------------
sql += "SELECT st.A AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE,"
sql += " 'Missing in TARGET' AS DIFF_COMMENT FROM (SELECT " + diff_key_sql_join_source + " AS A FROM SOURCE s MINUS "
sql += " SELECT " + diff_key_sql_join_target + " AS A FROM TARGET t) st"
sql += " UNION "
# --------------------------------------------

# ---------------------------------------------
# Records available in TARGET but not in SOURCE
# ---------------------------------------------
# Example:
#
# SELECT ts.B AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE, 'Missing in SOURCE' AS DIFF_COMMENT
# FROM (SELECT coalesce(t.EMP_ID,'')||':'||coalesce(t.EMP_NAME,'') AS B FROM TARGET t MINUS SELECT coalesce(s.EMP_ID,'')||':'||coalesce(s.EMP_NAME,'') AS B FROM SOURCE s) ts
# --------------------------------------------
sql += "SELECT ts.B AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE,"
sql += " 'Missing in SOURCE' AS DIFF_COMMENT FROM (SELECT " + diff_key_sql_join_target + " AS B FROM TARGET t MINUS "
sql += " SELECT " + diff_key_sql_join_source + " AS B FROM SOURCE s) ts"
sql += " UNION "
# --------------------------------------------

# ------------------------
# Duplicate SOURCE records
# ------------------------
# Example:
#
# SELECT coalesce(s.EMP_EMAIL,'')||':'||coalesce(s.EMP_PHONE,'') AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE, (COUNT(coalesce(s.EMP_EMAIL,'')||':'||coalesce(s.EMP_PHONE,''))-1)||' duplicate row(s) in SOURCE' AS DIFF_COMMENT
# FROM SOURCE s
# GROUP BY 1
# HAVING COUNT(coalesce(s.EMP_EMAIL,'')||':'||coalesce(s.EMP_PHONE,'')) > 1
# ------------------------
sql += "SELECT " + diff_key_sql_join_source + " AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE, "
sql += "(COUNT(" + diff_key_sql_join_source + ")-1)||' duplicate row(s) in SOURCE' AS DIFF_COMMENT "
sql += "FROM SOURCE s GROUP BY 1 HAVING COUNT(" + diff_key_sql_join_source + ") > 1"
sql += " UNION "
# ------------------------

# ------------------------
# Duplicate TARGET records
# ------------------------
# Example:
#
# SELECT coalesce(t.EMP_EMAIL,'')||':'||coalesce(t.EMP_PHONE,'') AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE, (COUNT(coalesce(t.EMP_EMAIL,'')||':'||coalesce(t.EMP_PHONE,''))-1)||' duplicate row(s) in TARGET' AS DIFF_COMMENT
# FROM TARGET t
# GROUP BY 1
# HAVING COUNT(coalesce(t.EMP_EMAIL,'')||':'||coalesce(t.EMP_PHONE,'')) > 1
# ------------------------
sql += "SELECT " + diff_key_sql_join_target + " AS KEY, NULL AS COLUMN, NULL AS SOURCE_VALUE, NULL AS TARGET_VALUE, "
sql += "(COUNT(" + diff_key_sql_join_target + ")-1)||' duplicate row(s) in TARGET' AS DIFF_COMMENT "
sql += "FROM TARGET t GROUP BY 1 HAVING COUNT(" + diff_key_sql_join_target + ") > 1"
# ------------------------

spark.sql(sql).show(truncate=False)
