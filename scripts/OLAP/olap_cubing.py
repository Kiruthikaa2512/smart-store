import sqlite3
import pandas as pd
import os

# Step 1: Set database and output file paths
db_path = "C:/Projects/smart-store-kiruthikaa/data/dw/smart_sales.db"
output_csv = "OLAP_Analysis/sales_cube.csv"

# Step 2: Connect to the database
conn = sqlite3.connect(db_path)

# Step 3: SQL query with correct column name 'paymenttype'
query = """
SELECT
    STRFTIME('%Y-%m', s.sale_date) AS month,
    c.region,
    p.category AS product_category,
    s.paymenttype AS payment_type,
    SUM(s.sale_amount) AS total_sales,
    COUNT(s.sale_id) AS transaction_count
FROM sale AS s
JOIN product AS p ON s.product_id = p.product_id
JOIN customer AS c ON s.customer_id = c.customer_id
GROUP BY month, c.region, p.category, s.paymenttype
ORDER BY month, total_sales DESC;
"""

# Step 4: Run the query and load results into pandas
cube_df = pd.read_sql_query(query, conn)

# Step 5: Close the database connection
conn.close()

# Step 6: Print first 5 rows to check data
print("Preview of Sales Cube:")
print(cube_df.head())

# Step 7: Create folder if it doesn't exist
os.makedirs("OLAP_Analysis", exist_ok=True)

# Step 8: Save the result to a CSV file
cube_df.to_csv(output_csv, index=False)

print("Sales cube saved successfully at:", output_csv)
