import sys
from pathlib import Path

# Ensure the project root is added to the Python search path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.spark_setup import get_spark_session  # Import Spark setup utility

def main():
    # Initialize Spark session
    spark = get_spark_session("UniversalApp")

    # Insert Spark queries directly here
    
    # Example code (commented out):
    # df = spark.createDataFrame(
    #     [("sue", 32), ("li", 3), ("bob", 75), ("heo", 13)], ["first_name", "age"]
    # )
    # df.show()

    spark.stop()

if __name__ == "__main__":
    main()