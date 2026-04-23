import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def preprocess_data(input_path, output_parquet_path):
    # Initialize Spark Session with Windows-specific fixes
    spark = SparkSession.builder \
        .appName("US Accidents Preprocessing") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()

    print(f"Loading dataset from {input_path}...")
    
    # Load CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    # Clean Column Names (Replace spaces and dots with underscores)
    new_columns = [c.replace(" ", "_").replace(".", "_").replace("(", "").replace(")", "") for c in df.columns]
    df = df.toDF(*new_columns)

    # Cast Start_Time and End_Time to Timestamp if they aren't already
    df = df.withColumn("Start_Time", col("Start_Time").cast("timestamp"))
    df = df.withColumn("End_Time", col("End_Time").cast("timestamp"))

    print(f"Schema after cleaning:")
    df.printSchema()

    # Save as Parquet for performance optimization
    print(f"Saving cleaned data to Parquet format at {output_parquet_path}...")
    df.write.mode("overwrite").parquet(output_parquet_path)
    
    print("Pre-processing complete.")
    spark.stop()

if __name__ == "__main__":
    DATA_FILE = "US_Accidents_March23.csv"
    OUTPUT_FILE = "data/us_accidents_cleaned.parquet"
    
    if os.path.exists(DATA_FILE):
        preprocess_data(DATA_FILE, OUTPUT_FILE)
    else:
        print(f"Error: {DATA_FILE} not found in the current directory.")
