import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf

def get_spark_session(app_name="MyApp", log_level="ERROR"):
    """
    Returns a pre-configured SparkSession with error handling
    
    Args:
        app_name (str): Name for the Spark application
        log_level (str): One of 'ALL', 'DEBUG', 'ERROR', 'FATAL', 'INFO', 'OFF', 'TRACE', 'WARN'
    
    Returns:
        SparkSession: Configured Spark session
    
    Raises:
        RuntimeError: If Spark initialization fails
    """
    try:
        # Set critical environment variables FIRST
        os.environ.update({
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_DRIVER_PYTHON': sys.executable,
            'SPARK_LOCAL_IP': '127.0.0.1'  # Prevents network resolution issues
        })
        
        # Configure Spark with fault-tolerant settings
        conf = SparkConf() \
            .setAppName(app_name) \
            .setMaster("local[*]") \
            .set("spark.sql.shuffle.partitions", "4") \
            .set("spark.driver.memory", "2g") \
            .set("spark.executor.memory", "2g") \
            .set("spark.ui.showConsoleProgress", "true") \
            .set("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .set("spark.driver.bindAddress", "127.0.0.1")
        
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        
        # Validate the session works
        spark.sparkContext.parallelize([1, 2, 3]).count()  # Simple health check
        
        return spark
        
    except Exception as e:
        error_msg = f"Spark initialization failed: {str(e)}"
        print(f"\n❌ {error_msg}", file=sys.stderr)
        print("Troubleshooting:")
        print(f"1. Python path: {sys.executable}")
        print("2. Verify Java is installed (java -version)")
        print("3. Check no other SparkContext is running")
        raise RuntimeError(error_msg) from e