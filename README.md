# Big Data Analytics: Spark Case Study - US Accidents Analysis

## 📌 Project Overview
This project performs a comprehensive analysis of the US Accidents dataset (2016-2023) using Apache Spark. It compares the performance and execution plans of the **RDD API**, **DataFrame API**, and **Spark SQL**.

## 📂 Project Structure
- `src/`: Source code files
  - `main.py`: Main entry point to run all 10 queries.
  - `query_implementations.py`: Detailed implementation of each query in 3 formats (RDD, DF, SQL).
  - `scalability_test.py`: Script to test performance with different partition counts.
  - `data_preprocessing.py`: Cleaning and format conversion logic.
- `data/`: Sample metadata and lookup files.
- `results/`: Contains performance metrics and `.explain()` execution plans.
- `report/`: (Placeholder for PDF) Final Project Report.

## 🚀 How to Run
1. **Prerequisites:** 
   - Apache Spark (v3.x+) installed.
   - Python 3.x with `pyspark` and `pandas`.
2. **Setup:**
   - Ensure `US_Accidents_March23.csv` is in the root directory.
3. **Execution:**
   - Run the main analysis: `python src/main.py`
   - Run the scalability test: `python src/scalability_test.py`
4. **View Results:**
   - Check `results/performance_metrics.csv` for timings.
   - Check `results/*.txt` for Spark execution plans.

## 📊 Key Findings
- **Structured APIs (DataFrame/SQL)** are significantly faster than RDDs due to the Catalyst Optimizer.
- **Broadcast Joins** outperform Sort-Merge joins for state-level lookups.
- **Window Functions** allow for advanced analytics like city rankings and moving averages.

## 👥 Authors
- Team Size: 2 Students
- Submitted: 25th April 2026
