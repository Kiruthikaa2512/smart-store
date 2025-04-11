# ====================== PYSPARK GOLDEN TEMPLATE ======================
# File: life_stage_demo.py
import sys
from pathlib import Path
from pyspark.sql.functions import col, when

# ==== ABSOLUTELY DO NOT MODIFY BELOW THIS LINE ====
sys.path.append(str(Path(__file__).parent.parent))  # Project root access

from utils.spark_setup import get_spark_session  # Central configuration

def execute_spark_job(spark):
    """
    ⚠️ ONLY EDIT INSIDE THIS FUNCTION (Between the triple quotes and return)
    """
    # ============== YOUR CODE GOES HERE ==============
    # Create initial DataFrame
    df = spark.createDataFrame(
        [
            ("sue", 32),
            ("li", 3),
            ("bob", 75),
            ("heo", 13)
        ],
        ["first_name", "age"]
    )
    
    # Add life_stage column
    df1 = df.withColumn(
        "life_stage",
        when(col("age") < 13, "child")
        .when(col("age").between(13, 19), "teenager")
        .otherwise("adult")
    )
    
    # Show results
    df1.show()
    # ============== END EDITABLE ZONE ==============

def main():
    spark = None
    try:
        spark = get_spark_session(Path(__file__).stem)  # Auto-names from filename
        execute_spark_job(spark)
    except Exception as e:
        print(f"❌ {Path(__file__).name} failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
        print(f"✅ {Path(__file__).name} completed")

if __name__ == "__main__":
    main()
# ==== ABSOLUTELY DO NOT MODIFY BELOW THIS LINE ====