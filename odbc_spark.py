import findspark
findspark.init()

import pyodbc
import pandas as pd
import time
import multiprocessing as mp
import shutil

from pathlib import Path
from pyspark.sql import SparkSession


# Function to write to parquet file in chunks
def chunk_csv(df, filename):
    print("Started writing to " + filename)
    df.to_parquet(filename, engine='fastparquet', compression='gzip', index=False)
    print(filename + " written successfully.")


if __name__ == '__main__':
    # Establish DB connection
    db = "D:\\Softwares\\SQLite\\sample-database-sqlite-1\\Data.db"
    conn = pyodbc.connect("DRIVER={SQLite3 ODBC Driver};DATABASE=" + db)
    
    # Parquet parquest file directory
    dir = "C:\\Users\\Vicky\\Desktop\\test_data"
    
    # Parquest file name
    output = dir + "\\chunk_"
    
    # Initiate Spark
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Remove directory containing parquest files if it exists (to avoid unwanted files due to errors in previous run)
    shutil.rmtree(dir, ignore_errors=True)
    
    # Create directory for storing parquest files
    Path(dir).mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    print("\nStart\n")

    df = pd.read_sql("SELECT count(*) FROM Products", conn)
    count = df.iloc[0, 0]
    print("Count from DB: " + str(count) + "\n")

    # Set max number of processes
    max_processes = 16
    
    # Calculate size of each chunk
    # chunksize = rowcount/max_processes, if rowcount is divisible by max_processes
    # chunksize = (rowcount/max_processes) + 1, if rowcount is not divisible by max_processes
    size = 0
    if count % max_processes == 0:
        size = count / max_processes
    else:
        size = (count / max_processes) + 1

    # Load data into Pandas DF using chunksize
    data = pd.read_sql("SELECT * FROM Products", conn, chunksize=int(size))

    # Spawn processes for multiprocessing
    ctx = mp.get_context('spawn')
    pool = ctx.Pool(max_processes)

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
    print("\nCount from Spark DF: " + str(spark_df.count()) + "\n")

    print("Complete\n")

    print("Time taken: " + str(time.time() - t0) + " seconds\n")
    
    # Remove directory containing parquest files at the end
    shutil.rmtree(dir, ignore_errors=True)
    
    spark.stop()
