import sqlite3
import pathlib

# Database path setup
DW_DIR = pathlib.Path(".venv").joinpath("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")

# Ensure 'data/dw' directory exists
DW_DIR.mkdir(parents=True, exist_ok=True)

def create_dw():
    """Create database and tables with correct column names."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop existing tables to ensure fresh creation (Optional)
    cursor.execute("DROP TABLE IF EXISTS sales;")
    cursor.execute("DROP TABLE IF EXISTS products;")
    cursor.execute("DROP TABLE IF EXISTS customers;")

    # Create customers table
    cursor.execute("""
    CREATE TABLE customers (
        CustomerID INTEGER PRIMARY KEY,
        Name TEXT NOT NULL,
        Region TEXT NOT NULL,
        JoinDate DATE NOT NULL,
        LoyaltyPoints INTEGER,
        PreferredContactMethod TEXT
    );
    """)

    # Create products table
    cursor.execute("""
    CREATE TABLE products (
        ProductID INTEGER PRIMARY KEY,
        ProductName TEXT NOT NULL,
        Category TEXT NOT NULL,
        UnitPrice REAL NOT NULL,
        StockQuantity INTEGER,
        Supplier TEXT
    );
    """)

    # Create sales table
    cursor.execute("""
    CREATE TABLE sales (
        TransactionID INTEGER PRIMARY KEY,
        SaleDate DATE NOT NULL,
        CustomerID INTEGER NOT NULL,
        ProductID INTEGER NOT NULL,
        StoreID INTEGER,
        CampaignID INTEGER,
        SaleAmount REAL NOT NULL,
        BonusPoints INTEGER,
        PaymentType TEXT,
        FOREIGN KEY (CustomerID) REFERENCES customers (CustomerID),
        FOREIGN KEY (ProductID) REFERENCES products (ProductID)
    );
    """)

    conn.commit()
    conn.close()

def convert_data_type(sqlite_type):
    """Convert SQLite types to user-friendly types."""
    type_mapping = {
        "TEXT": "TEXT",
        "REAL": "FLOAT",
        "INTEGER": "INTEGER",
        "DATE": "DATE"
    }
    return type_mapping.get(sqlite_type, sqlite_type)

def fetch_schema():
    """Fetch and display schemas with correct column names and descriptions."""
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
# Close the connection
    conn.close()

def get_description(table_name, column_name):
    """Return column descriptions aligned with provided data headers."""
    descriptions = {
        "customers": {
            "CustomerID": "Primary Key",
            "Name": "Customer's name",
            "Region": "Customer's region",
            "JoinDate": "Customer's join date",
            "LoyaltyPoints": "Customer's loyalty points",
            "PreferredContactMethod": "Customer's preferred contact method"
        },
        "products": {
            "ProductID": "Primary Key",
            "ProductName": "Product's name",
            "Category": "Product's category",
            "UnitPrice": "Product's price per unit",
            "StockQuantity": "Product's quantity in stock",
            "Supplier": "Product's supplier"
        },
        "sales": {
            "TransactionID": "Primary Key",
            "SaleDate": "Date of sale",
            "CustomerID": "Foreign key to customer",
            "ProductID": "Foreign key to product",
            "StoreID": "Store identifier",
            "CampaignID": "Campaign identifier",
            "SaleAmount": "Amount of sale",
            "BonusPoints": "Bonus points earned",
            "PaymentType": "Type of payment method"
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
