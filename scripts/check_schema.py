import sqlite3

conn = sqlite3.connect("C:/Projects/smart-store-kiruthikaa/data/dw/smart_sales.db")
cursor = conn.cursor()

# Check table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

# Check column names in sale table
cursor.execute("PRAGMA table_info(sale);")
columns = cursor.fetchall()
print("Columns in sale table:")
for col in columns:
    print(col)

conn.close()
