# Python Standard Library Imports
import sys
from pathlib import Path

# External imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Local module imports
from utils.logger import logger  # noqa: E402

def read_csv(spark: SparkSession, file_path: Path) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.
    Args:
        spark (SparkSession): The SparkSession object.
        file_path (Path): Path to the CSV file.
    Returns:
        DataFrame: The resulting Spark DataFrame.
    """

    logger.info(f"Attempting to read CSV file from: {file_path}")
    try:
        df = spark.read.csv(str(file_path), header=True, inferSchema=True)
        logger.info(f"Successfully read CSV file: {file_path}")
        logger.info(f"Schema of DataFrame:\n{df.printSchema()}")
        return df

    except AnalysisException as e:
        logger.error(f"Failed to read CSV file: {file_path}. Error: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error while reading CSV file: {file_path}. Error: {e}")
        raise