import findspark
findspark.init()

import pandas as pd
import time
import multiprocessing as mp
import shutil
import random
import subprocess

from pathlib import Path
from sqlalchemy import create_engine
from pyspark.sql import SparkSession


# Function to write data to parquet file in chunks
def chunk_parquet(df, filename):
    print("Started writing to " + filename)
    # Create parquet file using fastparquet engine and compress in gzip format
    df.to_parquet(filename, engine='fastparquet', compression='gzip', index=False)
    print(filename + " written successfully.")


# Function to get data from ODBC to spark
def odbc_to_spark(output, conn, count, max_processes, table, spark, hdfs_dir, local_dir, hdfs_namenode):
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
        # Save each chunk as a parquet file
        filename = output + str(chunk_num) + ".parquet"

        # Apply multiprocess
        pool.apply_async(chunk_parquet, args=(chunk, filename,))
        chunk_num += 1

    # Close multiprocessing pool
    pool.close()
    pool.join()

    # Copy parquet files from local to HDFS
    subprocess.run("hdfs dfs -copyFromLocal -f " + local_dir + " " + hdfs_namenode + "/", shell=True)
    print("\nParquet files copied to " + hdfs_dir + "\n")

    # Read data from parquet files from HDFS into spark
    spark_df = spark.read.parquet(hdfs_dir + "/*.parquet")
    print("\nCount from Spark DF (" + table + "): " + str(spark_df.count()) + "\n\n")

    return spark_df


# Main function
if __name__ == '__main__':
    # Establish DB connection using SQLAlchemy
    db = "D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Data.db"
    engine = create_engine("sqlite:///D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Data.db")
    conn = engine.connect().execution_options(stream_results=True)

    # Random number for appending to directory name
    rand = random.randint(100, 1000)

    # Parquet local file directory
    local_dir = "C:\\Users\\Vicky\\Desktop\\test_data_" + str(rand)

    # Parquet file name format
    output = local_dir + "\\chunk_"

    # Get HDFS namenode host and port
    hdfs_namenode = str(subprocess.check_output("hdfs getconf -confKey fs.defaultFS", shell=True).strip(), 'utf-8')

    # HDFS directory for parquet files
    hdfs_dir = hdfs_namenode + "/test_data_" + str(rand)

    # Set max number of processes
    max_processes = 32

    # Initiate Spark
    spark = SparkSession.builder.appName("ODBC_Spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\nHDFS namenode: " + hdfs_namenode + "\n")

    t0 = time.time()
    print("\nStart\n\n")

    for table in ["Products", "Sales"]:
        # Remove local directory containing parquet files (to avoid unwanted files due to errors in previous run)
        shutil.rmtree(local_dir, ignore_errors=True)
        
        # Remove hdfs directory for parquet files (to avoid unwanted files due to errors in previous run)
        result = str(subprocess.check_output("hdfs dfs -rm -r -f -skipTrash " + hdfs_dir, shell=True).strip(), 'utf-8')
        print(result)

        # Create local directory for storing parquet files
        Path(local_dir).mkdir(parents=True, exist_ok=True)

        # Count of rows
        df = pd.read_sql("SELECT count(*) FROM " + table, conn)
        count = df.iloc[0, 0]
        print("Count from DB (" + table + "): " + str(count) + "\n")

        odbc_to_spark(output, conn, count, max_processes, table, spark, hdfs_dir, local_dir, hdfs_namenode)

        # Remove hdfs directory for parquet files at the end
        result = str(subprocess.check_output("hdfs dfs -rm -r -f -skipTrash " + hdfs_dir, shell=True).strip(), 'utf-8')
        print(result + "\n")
        
        # Remove local directory containing parquet files at the end
        shutil.rmtree(local_dir, ignore_errors=True)

    print("\n\nComplete\n")
    print("Time taken: " + str(time.time() - t0) + " seconds\n")

    spark.stop()
