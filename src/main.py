import os
from pyspark.sql import SparkSession
from src.query_implementations import SparkQueryRunner

def main():
    # Initialize Spark Session with enough memory
    spark = SparkSession.builder \
        .appName("US Accidents Spark Case Study") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    input_csv = "US_Accidents_March23.csv"
    state_info_csv = "data/state_info.csv"

    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} not found.")
        return

    runner = SparkQueryRunner(spark, input_csv, state_info_csv)
    runner.run_all()

    spark.stop()

if __name__ == "__main__":
    main()
