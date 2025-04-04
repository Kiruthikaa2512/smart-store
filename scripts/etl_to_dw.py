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
DB_PATH = "scripts/.venv/data/smart_sales.db"  # Update this path if needed
CLEANED_DATA_DIR = pathlib.Path("data/cleaned")  # Directory for cleaned CSV files

# Table metadata: schema and file mapping
TABLE_METADATA = {
    "customer": {
        "file_name": "customer_data_cleaned.csv",
        "columns": {
            "CustomerID": "customer_id",
            "Name": "name",
            "Region": "region",
            "JoinDate": "join_date",
            "LoyaltyPoints": "loyaltypoints",
            "PreferredContactMethod": "preferredcontactmethod"
        },
        "schema": """
            CREATE TABLE customer (
                customer_id INTEGER PRIMARY KEY,
                name TEXT,
                region TEXT,
                join_date TEXT,
                loyaltypoints REAL CHECK(loyaltypoints >= 0),
                preferredcontactmethod TEXT
            )
        """
    },
    "product": {
        "file_name": "product_data_cleaned.csv",
        "columns": {
            "productid": "product_id",
            "productname": "product_name",
            "category": "category",
            "unitprice": "unitprice",
            "stockquantity": "stockquantity",
            "supplier": "supplier"
        },
        "schema": """
            CREATE TABLE product (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT,
                category TEXT,
                unitprice REAL CHECK(unitprice >= 0),
                stockquantity INTEGER CHECK(stockquantity >= 0),
                supplier TEXT
            )
        """
    },
    "sale": {
        "file_name": "sale_data_cleaned.csv",
        "columns": {
            "transactionid": "sale_id",
            "saledate": "sale_date",
            "customerid": "customer_id",
            "productid": "product_id",
            "storeid": "storeid",
            "campaignid": "campaignid",
            "saleamount": "sale_amount",
            "bonuspoints": "bonuspoints",
            "paymenttype": "paymenttype"
        },
        "schema": """
            CREATE TABLE sale (
                sale_id INTEGER PRIMARY KEY,
                sale_date TEXT,
                customer_id INTEGER,
                product_id INTEGER,
                storeid INTEGER,  -- Store ID column
                campaignid INTEGER,
                sale_amount REAL CHECK(sale_amount >= 0),
                bonuspoints REAL CHECK(bonuspoints >= 0),
                paymenttype TEXT,
                FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
                FOREIGN KEY (product_id) REFERENCES product (product_id)
            )
        """
    }
}

##############################################
# Helper Functions
##############################################

def create_tables(cursor: sqlite3.Cursor):
    """Drop and recreate all tables based on metadata."""
    for table_name, metadata in TABLE_METADATA.items():
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(metadata["schema"])

def load_and_prepare_data(table_name: str) -> pd.DataFrame:
    """Load and prepare data for the given table."""
    metadata = TABLE_METADATA[table_name]
    file_path = CLEANED_DATA_DIR / metadata["file_name"]
    df = pd.read_csv(file_path)
    df.rename(columns=metadata["columns"], inplace=True)
    df.dropna(how="all", inplace=True)  # Drop fully blank rows
    return df

def inject_data(table_name: str, df: pd.DataFrame, conn: sqlite3.Connection):
    """Inject data into the specified table."""
    try:
        print(f"Injecting data into '{table_name}'...")
        print(f"Columns: {df.columns.tolist()}")
        print(f"First 5 rows:\n{df.head()}")
        print(f"Total rows in '{table_name}' data: {len(df)}")

        df.to_sql(table_name, conn, if_exists="append", index=False)

        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Rows in '{table_name}' table after insertion: {row_count}")
    except Exception as e:
        print(f"Error inserting data into '{table_name}': {e}")

def display_table_contents(table_name: str, conn: sqlite3.Connection):
    """Display all contents of a given table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    print(f"Contents of table '{table_name}' ({len(rows)} rows):")
    for row in rows:
        print(row)

##############################################
# Main Workflow
##############################################

if __name__ == "__main__":
    try:
        # Connect to the database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Step 1: Create tables
        create_tables(cursor)

        # Step 2: Load, prepare, and inject data into tables
        for table_name in TABLE_METADATA.keys():
            df = load_and_prepare_data(table_name)
            inject_data(table_name, df, conn)

        # Step 3: Display contents of each table
        for table_name in TABLE_METADATA.keys():
            display_table_contents(table_name, conn)

        conn.commit()
        print("ETL process completed successfully!")
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
    finally:
        conn.close()