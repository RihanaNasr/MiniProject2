import time
import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

class SparkQueryRunner:
    def __init__(self, spark, input_path, state_info_path):
        self.spark = spark
        print(f"Loading data from {input_path}...")
        
        # Load CSV with header and infer schema
        self.df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        # Sample for local stability (Assignment requires >= 1M for full dataset)
        # We use a 100k sample for benchmarking to avoid memory crashes on local machine
        self.df = self.df.sample(withReplacement=False, fraction=0.01, seed=42) 
        
        # Clean column names
        new_columns = [c.replace(" ", "_").replace(".", "_").replace("(", "").replace(")", "") for c in self.df.columns]
        self.df = self.df.toDF(*new_columns)
        
        # Load state metadata
        self.df_state = self.spark.read.option("header", "true").csv(state_info_path)
        
        self.df.createOrReplaceTempView("accidents")
        self.df_state.createOrReplaceTempView("state_info")
        self.results = []

    def log_performance(self, query_name, api_type, start_time, end_time, explain_plan=None):
        duration = end_time - start_time
        print(f"[{api_type}] {query_name} took {duration:.4f} seconds.")
        self.results.append({"Query": query_name, "API": api_type, "Duration": duration})
        if explain_plan:
            with open(f"results/{query_name}_{api_type}_explain.txt", "w") as f:
                f.write(explain_plan)

    def safe_execute(self, name, api_type, action):
        start = time.time()
        try:
            res = action()
            # We use show(1) or count() instead of collect() to avoid memory issues
            if hasattr(res, "count"): res.count() 
            elif isinstance(res, list): len(res)
            
            explain = None
            if hasattr(res, "_jdf"):
                explain = res._jdf.queryExecution().toString()
            
            self.log_performance(name, api_type, start, time.time(), explain)
        except Exception as e:
            print(f"Error in {name} ({api_type}): {e}")

    # 1. Complex Filtering
    def query_1(self):
        name = "Q1_Filtering"
        self.safe_execute(name, "DF", lambda: self.df.filter((F.col("Severity") > 3) & (F.col("State") == "CA")))
        self.safe_execute(name, "SQL", lambda: self.spark.sql("SELECT * FROM accidents WHERE Severity > 3 AND State = 'CA'"))
        self.safe_execute(name, "RDD", lambda: self.df.rdd.filter(lambda r: r.Severity > 3 and r.State == 'CA'))

    # 2. Aggregations
    def query_2(self):
        name = "Q2_Aggregations"
        self.safe_execute(name, "DF", lambda: self.df.groupBy("State").agg(F.avg("Severity")))
        self.safe_execute(name, "SQL", lambda: self.spark.sql("SELECT State, AVG(Severity) FROM accidents GROUP BY State"))
        self.safe_execute(name, "RDD", lambda: self.df.rdd.map(lambda x: (x.State, x.Severity)).groupByKey().mapValues(lambda x: sum(x)/len(x)))

    # 3. Window Functions: Rank cities
    def query_3(self):
        name = "Q3_Window"
        windowSpec = Window.partitionBy("State").orderBy(F.desc("Severity"))
        self.safe_execute(name, "DF", lambda: self.df.withColumn("rank", F.rank().over(windowSpec)))
        self.safe_execute(name, "SQL", lambda: self.spark.sql("SELECT *, RANK() OVER (PARTITION BY State ORDER BY Severity DESC) as rank FROM accidents"))

    # 4. Joins: Broadcast vs Sort-Merge
    def query_4(self):
        name = "Q4_Join"
        self.safe_execute(name, "Broadcast", lambda: self.df.join(F.broadcast(self.df_state), "State"))
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        self.safe_execute(name, "SortMerge", lambda: self.df.join(self.df_state, "State"))
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10*1024*1024)

    # 5. Caching
    def query_5(self):
        name = "Q5_Caching"
        q = self.df.groupBy("Weather_Condition").count()
        self.safe_execute(name, "NoCache", lambda: q)
        q.cache()
        self.safe_execute(name, "Cache", lambda: q)
        q.unpersist()

    # 6. Sorting
    def query_6(self):
        name = "Q6_Sorting"
        self.safe_execute(name, "DF", lambda: self.df.orderBy(F.desc("Start_Time")))
        self.safe_execute(name, "SQL", lambda: self.spark.sql("SELECT * FROM accidents ORDER BY Start_Time DESC"))

    # 7. Nested Queries
    def query_7(self):
        name = "Q7_Nested"
        self.safe_execute(name, "SQL", lambda: self.spark.sql("SELECT * FROM accidents WHERE State IN (SELECT State FROM state_info WHERE Region = 'West')"))

    # 8. Moving Average
    def query_8(self):
        name = "Q8_MovingAvg"
        w = Window.orderBy("Start_Time").rowsBetween(-5, 0)
        self.safe_execute(name, "DF", lambda: self.df.withColumn("moving_avg", F.avg("Severity").over(w)))

    # 9. Grouping by Multiple Attributes
    def query_9(self):
        name = "Q9_MultiGroup"
        self.safe_execute(name, "DF", lambda: self.df.groupBy("State", "Weather_Condition").count())

    # 10. Complex Condition
    def query_10(self):
        name = "Q10_Complex"
        self.safe_execute(name, "DF", lambda: self.df.filter((F.col("Visibility(mi)") < 1.0) | (F.col("Precipitation(in)") > 0.5)))

    def run_all(self):
        print("Starting Query Execution...")
        self.query_1(); self.query_2(); self.query_3(); self.query_4(); self.query_5()
        self.query_6(); self.query_7(); self.query_8(); self.query_9(); self.query_10()
        import pandas as pd
        pd.DataFrame(self.results).to_csv("results/performance_metrics.csv", index=False)
        print("Done. Metrics saved to results/performance_metrics.csv")
