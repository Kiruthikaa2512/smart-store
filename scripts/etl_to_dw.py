import pandas as pd
import sqlite3
import pathlib
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_sales.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Paths
BASE_DIR = pathlib.Path(r"C:\Projects\smart-store-kiruthikaa")
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
DB_PATH = BASE_DIR / "data" / "smart_store_sales.db"

# Sale table configuration
SALE_TABLE = {
    "csv": "sale_data_cleaned.csv",
    "columns": [
        "transactionid",
        "saledate",
        "customerid",
        "productid",
        "storeid",
        "campaignid",
        "saleamount",
        "bonuspoints",
        "paymenttype"
    ],
    "schema": """
        CREATE TABLE sale (
            sale_id TEXT PRIMARY KEY,
            sale_date TEXT NOT NULL,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            store_id INTEGER,
            campaign_id INTEGER,
            sale_amount REAL CHECK(sale_amount >= 0),
            bonus_points REAL DEFAULT 0,
            payment_type TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
            FOREIGN KEY (product_id) REFERENCES product(product_id)
        )
    """,
    "column_mapping": {
        "transactionid": "sale_id",
        "saledate": "sale_date",
        "customerid": "customer_id",
        "productid": "product_id",
        "storeid": "store_id",
        "campaignid": "campaign_id",
        "saleamount": "sale_amount",
        "bonuspoints": "bonus_points",
        "paymenttype": "payment_type"
    }
}

def verify_prerequisite_tables(conn):
    """Check if customer and product tables exist and have data."""
    cursor = conn.cursor()
    try:
        # Check customer table
        cursor.execute("SELECT COUNT(*) FROM customer")
        customer_count = cursor.fetchone()[0]
        logging.info(f"Customer records found: {customer_count}")
        
        # Check product table
        cursor.execute("SELECT COUNT(*) FROM product")
        product_count = cursor.fetchone()[0]
        logging.info(f"Product records found: {product_count}")
        
        return customer_count > 0 and product_count > 0
    except sqlite3.Error as e:
        logging.error(f"Error checking prerequisite tables: {str(e)}")
        return False

def load_sales_data():
    """Load and prepare sales data with validation."""
    csv_path = CLEANED_DATA_DIR / SALE_TABLE["csv"]
    
    if not csv_path.exists():
        logging.error(f"Sales data file not found: {csv_path}")
        return None

    try:
        # Load CSV with specific columns
        df = pd.read_csv(
            csv_path,
            usecols=SALE_TABLE["columns"],
            dtype={
                'transactionid': str,
                'customerid': int,
                'productid': int,
                'storeid': int,
                'campaignid': int
            },
            parse_dates=['saledate']
        )
        
        # Verify all required columns are present
        missing_cols = set(SALE_TABLE["columns"]) - set(df.columns)
        if missing_cols:
            logging.error(f"Missing columns in sales data: {missing_cols}")
            return None
            
        logging.info(f"Loaded {len(df)} sales records from {csv_path.name}")
        return df

    except Exception as e:
        logging.error(f"Error loading sales data: {str(e)}")
        return None

def validate_foreign_keys(conn, sales_df):
    """Validate that all foreign keys exist in their parent tables."""
    cursor = conn.cursor()
    
    # Check customer IDs
    cursor.execute("SELECT customer_id FROM customer")
    valid_customers = {row[0] for row in cursor.fetchall()}
    invalid_customers = set(sales_df['customerid']) - valid_customers
    
    # Check product IDs
    cursor.execute("SELECT product_id FROM product")
    valid_products = {row[0] for row in cursor.fetchall()}
    invalid_products = set(sales_df['productid']) - valid_products
    
    if invalid_customers:
        logging.warning(f"Found {len(invalid_customers)} invalid customer references")
        logging.debug(f"Invalid customer IDs: {list(invalid_customers)[:10]}")
    
    if invalid_products:
        logging.warning(f"Found {len(invalid_products)} invalid product references")
        logging.debug(f"Invalid product IDs: {list(invalid_products)[:10]}")
    
    return not (invalid_customers or invalid_products)

def insert_sales_data(conn, sales_df):
    """Insert validated sales data into database."""
    if sales_df is None or sales_df.empty:
        logging.error("No sales data to insert")
        return False

    try:
        # Rename columns to match database schema
        sales_df = sales_df.rename(columns=SALE_TABLE["column_mapping"])
        
        # Convert sale_date to string format
        sales_df['sale_date'] = sales_df['sale_date'].dt.strftime('%Y-%m-%d')
        
        # Insert data
        sales_df.to_sql(
            "sale",
            conn,
            if_exists="append",
            index=False,
            dtype={
                'sale_date': 'TEXT',
                'payment_type': 'TEXT'
            }
        )
        
        # Verify insertion
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sale")
        count = cursor.fetchone()[0]
        logging.info(f"Successfully inserted {len(sales_df)} sales records. Total now: {count}")
        return True
        
    except Exception as e:
        logging.error(f"Error inserting sales data: {str(e)}")
        return False

def main():
    """Main ETL workflow for sales data."""
    conn = None
    try:
        # Initialize database connection
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Verify prerequisite tables
        if not verify_prerequisite_tables(conn):
            logging.error("Customer or product tables are missing or empty")
            return
        
        # Create sale table if it doesn't exist
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS sale")
        cursor.execute(SALE_TABLE["schema"])
        logging.info("Created sale table")
        
        # Load and validate sales data
        sales_df = load_sales_data()
        if sales_df is None:
            logging.error("Failed to load sales data")
            return
            
        # Validate foreign keys
        if not validate_foreign_keys(conn, sales_df):
            logging.error("Foreign key validation failed")
            return
            
        # Insert data
        if insert_sales_data(conn, sales_df):
            conn.commit()
            logging.info("Sales data loaded successfully!")
        else:
            conn.rollback()
            
    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()