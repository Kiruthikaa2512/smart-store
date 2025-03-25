"""
scripts/data_preparation/prepare_products_data.py

This script reads product data from the data/raw folder, cleans the data, 
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

-----------------------------------
How to Run:
1. Open a terminal in the main root project folder.
2. Activate the local project virtual environment.
3. Choose the correct commands for your OS to run this script:

Example (Windows/PowerShell) - do NOT include the > prompt:
> .venv\Scripts\activate
> py scripts\data_preparation\prepare_products_data.py

Example (Mac/Linux) - do NOT include the $ prompt:
$ source .venv/bin/activate
$ python3 scripts/data_preparation/prepare_products_data.py
"""

import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger  # noqa: E402

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

# -------------------
# Reusable Functions
# -------------------

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    logger.info(f"Reading raw data from {RAW_DATA_DIR / file_name}")
    return pd.read_csv(RAW_DATA_DIR / file_name)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV."""
    file_path = PREPARED_DATA_DIR / file_name
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows based on 'ProductID' and 'ProductName'."""
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    df = df.drop_duplicates(subset=["ProductID", "ProductName"])
    logger.info(f"Dataframe shape after removing duplicates: {df.shape}")
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values by dropping rows with missing 'ProductName' or 'UnitPrice'."""
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")
    df = df.dropna(subset=["ProductName", "UnitPrice"])
    logger.info(f"Dataframe shape after handling missing values: {df.shape}")
    return df

def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers based on thresholds for 'UnitPrice' and 'StockQuantity'."""
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    df = df[(df["UnitPrice"] > 20) & (df["UnitPrice"] <= 800)]
    df = df[df["StockQuantity"] <= 1000]
    logger.info(f"Dataframe shape after removing outliers: {df.shape}")
    return df

def exclude_last_duplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Exclude the specific row with duplicate 'ProductID' 109."""
    logger.info(f"FUNCTION START: exclude_last_duplicate with dataframe shape={df.shape}")
    df = df[~((df["ProductID"] == 109) & (df["Supplier"] == "HCL"))]
    logger.info(f"Dataframe shape after excluding last duplicate: {df.shape}")
    return df

def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize the formatting of various columns."""
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")
    df["ProductName"] = df["ProductName"].str.title()  # Title case for product names
    df["Category"] = df["Category"].str.lower()  # Lowercase for categories
    logger.info(f"Dataframe shape after standardizing formats: {df.shape}")
    return df

# -------------------
# Main Function
# -------------------

def main() -> None:
    """Main function for processing product data."""
    logger.info("==================================")
    logger.info("STARTING prepare_products_data.py")
    logger.info("==================================")

    input_file = "products_data.csv"
    output_file = "products_data_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Clean data
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = remove_outliers(df)
    df = exclude_last_duplicate(df)  # Exclude the duplicate row with ProductID 109
    df = standardize_formats(df)

    # Save cleaned data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info("FINISHED prepare_products_data.py")
    logger.info("==================================")

# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()
