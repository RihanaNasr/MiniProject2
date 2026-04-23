import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def run_scalability_test(input_path):
    # Testing different partition counts to simulate different cluster scales
    partition_counts = [2, 4, 8, 16]
    results = []

    for p in partition_counts:
        spark = SparkSession.builder \
            .appName(f"Scalability Test - Partitions {p}") \
            .config("spark.sql.shuffle.partitions", str(p)) \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        # Use a consistent 1M row sample for testing
        df_sample = df.limit(1000000).repartition(p)
        
        start_time = time.time()
        
        # A shuffle-intensive aggregation to test scalability
        result = df_sample.groupBy("State").count().orderBy("count")
        result.collect()
        
        duration = time.time() - start_time
        print(f"Partitions: {p} | Execution Time: {duration:.4f}s")
        results.append({"Partitions": p, "ExecutionTime": duration})
        
        spark.stop()

    # Save results
    import pandas as pd
    pd.DataFrame(results).to_csv("results/scalability_results.csv", index=False)
    print("Scalability test complete. Results saved to results/scalability_results.csv")

if __name__ == "__main__":
    run_scalability_test("US_Accidents_March23.csv")
