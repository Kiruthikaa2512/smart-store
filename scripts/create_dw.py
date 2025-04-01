import sqlite3
import pathlib

# Database path setup
DW_DIR = pathlib.Path(".venv").joinpath("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")

# Ensure 'data/dw' directory exists
DW_DIR.mkdir(parents=True, exist_ok=True)

def create_dw():
    """Create database and tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create customers table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        region TEXT NOT NULL,
        join_date DATE NOT NULL,
        total_transactions INTEGER,
        loyalty_status TEXT
    );
    """)

    # Create products table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        unit_price REAL NOT NULL,
        days_to_receive INTEGER,
        customizable TEXT
    );
    """)

    # Create sales table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        sale_id INTEGER PRIMARY KEY,
        date DATE NOT NULL,
        customer_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        store_id INTEGER,
        campaign_id INTEGER,
        quantity INTEGER NOT NULL,
        sales_amount REAL NOT NULL,
        bill_type TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    );
    """)

    conn.commit()
    conn.close()

def convert_data_type(sqlite_type):
    """Convert SQLite types to user-friendly types."""
    type_mapping = {
        "TEXT": "STR",
        "REAL": "FLOAT",
        "INTEGER": "INTEGER",
        "DATE": "DATE"
    }
    return type_mapping.get(sqlite_type, sqlite_type)

def fetch_schema():
    """Fetch and display schemas."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    tables = ["customers", "products", "sales"]

    for table in tables:
        print(f"\nSchema for **{table.capitalize()} Table**:")
        print("| Column Name         | Data Type | Description                       |")
        print("|---------------------|-----------|-----------------------------------|")

        # Use PRAGMA to fetch schema
        cursor.execute(f"PRAGMA table_info({table});")
        schema = cursor.fetchall()

        for col in schema:
            column_name = col[1]
            sqlite_type = col[2]
            data_type = convert_data_type(sqlite_type)
            description = get_description(table, column_name)
            print(f"| {column_name:<20} | {data_type:<9} | {description:<33} |")

    conn.close()

def get_description(table_name, column_name):
    """Return column descriptions."""
    descriptions = {
        "customers": {
            "customer_id": "Primary Key",
            "name": "Customer's name",
            "region": "Customer's region",
            "join_date": "Customer's Join Date",
            "total_transactions": "Customer's loyalty points",
            "loyalty_status": "Customer's preferred contact method"
        },
        "products": {
            "product_id": "Primary Key",
            "product_name": "Product's description",
            "category": "Product's category",
            "unit_price": "Product's price per unit",
            "days_to_receive": "Product's quantity in stock",
            "customizable": "Product's supplier"
        },
        "sales": {
            "sale_id": "Primary Key",
            "date": "Sale date",
            "customer_id": "Foreign key to customer",
            "product_id": "Foreign key to product",
            "store_id": "Sale location",
            "campaign_id": "Sale campaign identifier",
            "quantity": "Quantity sold",
            "sales_amount": "Sale amount",
            "bill_type": "Sale payment method"
        }
    }
    return descriptions.get(table_name, {}).get(column_name, "No description available")

def main():
    """Run the full workflow."""
    create_dw()
    fetch_schema()
    input("\nPress Enter to exit...")  # Keep terminal open for output visibility

if __name__ == "__main__":
    main()