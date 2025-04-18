import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Connect to the database
conn = sqlite3.connect("C:/Projects/smart-store-kiruthikaa/data/dw/smart_sales.db")
query = "SELECT customer_id, sale_id FROM sale;"
df = pd.read_sql_query(query, conn)
conn.close()

# Step 2: Count purchases by each customer
purchase_counts = df.groupby('customer_id').size().reset_index(name='purchase_count')

# Step 3: Filter repeat customers
repeat_customers = purchase_counts[purchase_counts['purchase_count'] > 1]

# Step 4: Plot
plt.figure(figsize=(10, 6))
plt.bar(repeat_customers['customer_id'].astype(str), repeat_customers['purchase_count'], color='green')
plt.title('Repeat Customers Purchase Count')
plt.xlabel('Customer ID')
plt.ylabel('Number of Purchases')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()
