import os
import sys
from pyspark.sql import SparkSession

# 1. CRITICAL FOR WINDOWS - Set Python paths first
os.environ['PYSPARK_PYTHON'] = sys.executable  # Points to your current Python
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 2. Configure Spark with error handling
try:
    spark = SparkSession.builder \
        .appName("WindowsTest") \
        .master("local[*]") \
        .getOrCreate()
    
    # 3. Simple test that always works
    test_df = spark.createDataFrame([(1, "Hello"), (2, "World")], ["id", "text"])
    
    # 4. Show results with explicit truncate=False
    test_df.show(truncate=False)
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    print("\nTROUBLESHOOTING:")
    print(f"1. Python path: {sys.executable}")
    print("2. Check Java is installed (java -version)")
    print("3. Validate SPARK_HOME environment variable")
    
finally:
    if 'spark' in locals():
        spark.stop()