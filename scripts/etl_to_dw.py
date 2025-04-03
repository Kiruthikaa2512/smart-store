import pandas as pd
import sqlite3
import pathlib
import logging

# Logging setup
logging.basicConfig(
    filename="logs/project_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
DB_PATH = pathlib.Path("scripts/.venv/data/smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data/prepared")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables with schemas matching the CSV files."""
    cursor.execute("DROP TABLE IF EXISTS customer")
    cursor.execute("DROP TABLE IF EXISTS product")
    cursor.execute("DROP TABLE IF EXISTS sale")

    # Customer table schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            region TEXT,
            join_date TEXT,
            loyaltypoints REAL,
            preferredcontactmethod TEXT
        )
    """)

    # Product table schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unitprice REAL,
            stockquantity INTEGER,
            supplier TEXT
        )
    """)

    # Sale table schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sale (
            sale_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            sale_date TEXT,
            storeid TEXT,
            campaignid TEXT,
            sale_amount REAL,
            bonuspoints REAL,
            paymenttype TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
            FOREIGN KEY (product_id) REFERENCES product (product_id)
        )
    """)

def preprocess_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Preprocess data: fix IDs, handle NaN values, and validate completeness."""
    if table_name == "customer":
        df.rename(columns={"CustomerID": "customer_id", "Name": "name", "Region": "region", "JoinDate": "join_date", "LoyaltyPoints": "loyaltypoints", "PreferredContactMethod": "preferredcontactmethod"}, inplace=True)
        df["customer_id"] = df["customer_id"].fillna(pd.Series(range(1, len(df) + 1))).astype(int)
    elif table_name == "product":
        df.rename(columns={"productid": "product_id", "productname": "product_name", "category": "category", "unitprice": "unitprice", "stockquantity": "stockquantity", "supplier": "supplier"}, inplace=True)
        df["product_id"] = df["product_id"].fillna(pd.Series(range(1, len(df) + 1))).astype(int)

        # Deduplicate product_id column
        df.drop_duplicates(subset=["product_id"], inplace=True)
    elif table_name == "sale":
        df.rename(columns={"transactionid": "sale_id", "saledate": "sale_date", "customerid": "customer_id", "productid": "product_id", "storeid": "storeid", "campaignid": "campaignid", "saleamount": "sale_amount", "bonuspoints": "bonuspoints", "paymenttype": "paymenttype"}, inplace=True)
        df["sale_id"] = df["sale_id"].fillna(pd.Series(range(1, len(df) + 1))).astype(int)

    # Fill NaN values with sensible defaults
    df.fillna({
        "loyaltypoints": 0.0,
        "preferredcontactmethod": "Unknown",
        "unitprice": 0.0,
        "stockquantity": 0,
        "supplier": "Unknown",
        "sale_amount": 0.0,
        "bonuspoints": 0.0,
        "paymenttype": "Unknown",
    }, inplace=True)

    return df

def inject_data_to_table(csv_file: pathlib.Path, table_name: str, conn: sqlite3.Connection) -> None:
    """Inject data into tables with preprocessing and validation."""
    try:
        if not csv_file.exists():
            raise FileNotFoundError(f"{csv_file} not found!")

        df = pd.read_csv(csv_file)

        # Preprocess data
        df = preprocess_data(df, table_name)

        # Debugging: Log DataFrame preview
        print(f"\n--- Preview for {table_name} ---")
        print(df.info())  # Detailed column info
        print(df.head())  # First few rows for visual validation

        # Insert data into the database
        df.to_sql(table_name, conn, if_exists="append", index=False)
        print(f"Data successfully injected into {table_name}.")
    except Exception as e:
        print(f"Failed to inject data into {table_name}: {e}")
        logging.error(f"Failed to inject data into {table_name}: {e}")

def load_data_to_db() -> None:
    """Main function to create schema and load data."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create table schemas
        create_schema(cursor)

        # Inject data into tables
        inject_data_to_table(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"), "customer", conn)
        inject_data_to_table(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"), "product", conn)
        inject_data_to_table(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"), "sale", conn)

        conn.commit()
        print("All data successfully loaded into the database!")
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()