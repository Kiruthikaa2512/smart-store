import sys
from pathlib import Path

# Ensure the project root is added to the Python search path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.spark_setup import get_spark_session  # Import Spark setup utility
from pyspark.sql.functions import avg  # Ensure this import is not indented

def main():
    # Initialize Spark session
    spark = get_spark_session("UniversalApp")

    # Insert Spark queries directly here

    # Example code:
    df = spark.createDataFrame(
        [("sue", 32), ("li", 3), ("bob", 75), ("heo", 13)], ["first_name", "age"]
    )

    # Show average age
    df.select(avg("age")).show()

    # Show the DataFrame
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()