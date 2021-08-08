import findspark
findspark.init()

import pyodbc
import pandas as pd
import time
import multiprocessing as mp
import shutil

from pathlib import Path
from pyspark.sql import SparkSession


# Function to write data to parquet file in chunks
def chunk_csv(df, filename):
    print("Started writing to " + filename)
    # Create parquet file using fastparquet engine and compress in gzip format
    df.to_parquet(filename, engine='fastparquet', compression='gzip', index=False)
    print(filename + " written successfully.")


# Function to get data from ODBC to spark
def odbc_to_spark(dir, output, conn, count, max_processes, table, spark):
    # Remove directory containing parquet files if it exists (to avoid unwanted files due to errors in previous run)
    shutil.rmtree(dir, ignore_errors=True)

    # Create directory for storing parquet files
    Path(dir).mkdir(parents=True, exist_ok=True)

    # Calculate size of each chunk
    # chunksize = rowcount/max_processes, if rowcount is divisible by max_processes
    # chunksize = (rowcount/max_processes) + 1, if rowcount is not divisible by max_processes
    size = 0
    if count % max_processes == 0:
        size = count / max_processes
    else:
        size = (count / max_processes) + 1

    # Spawn processes for multiprocessing
    ctx = mp.get_context('spawn')
    pool = ctx.Pool(max_processes)

    # Load data into Pandas DF using chunksize
    data = pd.read_sql("SELECT * FROM " + table, conn, chunksize=int(size))

    chunk_num = 1
    for chunk in data:
        # Process each data frame
        filename = output + str(chunk_num) + ".parquet"
        # Multiprocess
        pool.apply_async(chunk_csv, args=(chunk, filename,))
        chunk_num += 1

    # Close multiprocessing pool
    pool.close()
    pool.join()

    # Read data from parquet files into spark
    spark_df = spark.read.parquet(output + "*.parquet")
    print("\nCount from Spark DF (" + table + "): " + str(spark_df.count()) + "\n\n")

    # Remove directory containing parquet files at the end
    shutil.rmtree(dir, ignore_errors=True)

    return spark_df


# Main function
if __name__ == '__main__':
    # Establish DB connection
    db = "D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Data.db"
    conn = pyodbc.connect("DRIVER={SQLite3 ODBC Driver};DATABASE=" + db)
    
    # Parquet file directory
    dir = "C:\\Users\\Vicky\\Desktop\\test_data"
    
    # Parquet file name format
    output = dir + "\\chunk_"

    # Set max number of processes
    max_processes = 16

    # Initiate Spark
    spark = SparkSession.builder.appName("ODBC_Spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    t0 = time.time()
    print("\nStart\n\n")

    for table in ["Products", "Sales"]:
        # Count of rows
        df = pd.read_sql("SELECT count(*) FROM " + table, conn)
        count = df.iloc[0, 0]
        print("Count from DB (" + table + "): " + str(count) + "\n")

        odbc_to_spark(dir, output, conn, count, max_processes, table, spark)

    print("Complete\n")
    print("Time taken: " + str(time.time() - t0) + " seconds\n")

    spark.stop()
