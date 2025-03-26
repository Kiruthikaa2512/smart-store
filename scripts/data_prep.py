import pathlib
import sys
import pandas as pd

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import local modules
from utils.logger import logger  # noqa: E402
from scripts.data_scrubber import DataScrubber  # noqa: E402

DATA_DIR = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR = DATA_DIR.joinpath("prepared")

# Allowed Payment Types
VALID_PAYMENT_TYPES = {"Credit", "Debit", "Cash"}

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV and standardize column names."""
    file_path = RAW_DATA_DIR.joinpath(file_name)
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.lower()  # Standardize column names
        logger.info(f"Successfully read {file_name}.")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Empty DataFrame fallback
    except Exception as e:
        logger.error(f"Error reading {file_name}: {e}")
        return pd.DataFrame()  # Empty DataFrame fallback


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV."""
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


def clean_customers_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess customers data."""
    df = df.drop_duplicates()

    # Fix names
    df["name"] = df["name"].str.strip().replace({
        "Xys": "Anonymous", 
        "Unknown": "Anonymous", 
        "Hermione Grager": "Hermione Granger"
    }).fillna("Anonymous")

    # Aggregate duplicates
    df = df.groupby("name", as_index=False).agg({
        "customerid": "first",
        "loyaltypoints": "sum",
        "joindate": "first"
    })

    # Standardize dates
    df["joindate"] = pd.to_datetime(df["joindate"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["joindate"])

    logger.info(f"Cleaned customers data: {df.shape[0]} rows remain.")
    return df


def clean_products_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess products data."""
    df = df.drop_duplicates()

    # Handle duplicates across all columns
    df = df.drop_duplicates(subset=None, keep="first")  # Ensures no duplicates remain

    # Handle StockQuantity
    df["stockquantity"] = pd.to_numeric(df["stockquantity"], errors="coerce").fillna(0)
    df = df.loc[(df["stockquantity"] >= 0) & (df["stockquantity"] <= 1500)]  # Removes outliers

    # Handle Supplier Typos
    df["supplier"] = df["supplier"].replace({"Blooom": "Bloom"})

    logger.info(f"Cleaned products data: {df.shape[0]} rows remain.")
    return df


def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess sales data."""
    df = df.drop_duplicates()

    # Required columns check
    required_columns = {"transactionid", "saledate", "saleamount", "bonuspoints", "paymenttype"}
    missing = required_columns - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns in sales_data.csv: {missing}")

    # Standardize payment types
    df["paymenttype"] = df["paymenttype"].str.capitalize().replace({"Dbit": "Debit"})
    valid_payment_types = {"Cash", "Debit", "Credit"}
    df = df[df["paymenttype"].isin(valid_payment_types)]

    # Handle BonusPoints
    df["bonuspoints"] = pd.to_numeric(df["bonuspoints"], errors="coerce")
    df["bonuspoints"] = df["bonuspoints"].fillna(0)  # Replace missing BonusPoints with 0
    df.loc[df["bonuspoints"] < 0, "bonuspoints"] = 0  # Replace negative BonusPoints with 0

    # Filter SaleAmount (1.0 - 8000.0)
    df["saleamount"] = pd.to_numeric(df["saleamount"], errors="coerce").fillna(0)
    df = df[(df["saleamount"] > 0) & (df["saleamount"] <= 8000)]  # Removes outliers

    # Standardize SaleDate
    df["saledate"] = pd.to_datetime(df["saledate"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["saledate"])  # Removes invalid dates

    logger.info(f"Cleaned sales data: {df.shape[0]} rows remain.")
    return df


def main():
    """Main function to run the data cleaning pipeline."""
    logger.info("=== STARTING DATA PREP ===")

    # Process customers
    logger.info("Processing CUSTOMERS data...")
    df_customers = read_raw_data("customers_data.csv")
    df_customers_cleaned = clean_customers_data(df_customers)
    save_prepared_data(df_customers_cleaned, "customers_data_prepared.csv")

    # Process products
    logger.info("Processing PRODUCTS data...")
    df_products = read_raw_data("products_data.csv")
    df_products_cleaned = clean_products_data(df_products)
    save_prepared_data(df_products_cleaned, "products_data_prepared.csv")

    # Process sales
    logger.info("Processing SALES data...")
    df_sales = read_raw_data("sales_data.csv")
    df_sales_cleaned = clean_sales_data(df_sales)
    save_prepared_data(df_sales_cleaned, "sales_data_prepared.csv")

    logger.info("=== DATA PREP FINISHED ===")


if __name__ == "__main__":
    main()
