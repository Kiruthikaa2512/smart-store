import sqlite3

try:
    conn = sqlite3.connect("C:/Projects/smart-store-kiruthikaa/scripts/.venv/data/smart_sales.db")
    print("Database connection successful!")
    conn.close()
except sqlite3.OperationalError as e:
    print(f"Error: {e}")