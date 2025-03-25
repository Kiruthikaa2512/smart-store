"""
Module 3: Cleanup sales csv files and write to new csv file
"""


import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREP_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"Reading raw data from {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs
    
def save_prepared_data(df, file_name):
    """Save cleaned data to csv"""
    file_path: pathlib.Path = PREP_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def process_data(file_name: str) -> None:
    """Process raw data by reading it into a pandas DataFrame object."""
    df = read_raw_data(file_name)
    df.columns = df.columns.str.strip()  # Clean column names
    df = df.drop_duplicates()            # Remove duplicates
    df['SaleAmount']=df['SaleAmount'].round(2)
    df = df.dropna(subset=['CustomerID'])  # Drop rows missing critical info

    return df

def main() -> None:
    """Main function for processing customer data."""
    logger.info("Starting data preparation...")
    cleaned_df=process_data("sales_data.csv")
    save_prepared_data(cleaned_df, "sales_data_prepared.csv")
  
    logger.info("Data preparation complete.")

if __name__ == "__main__":
    main()