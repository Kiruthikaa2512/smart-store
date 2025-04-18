import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Connect to the database and get the data
connection = sqlite3.connect("C:/Projects/smart-store-kiruthikaa/data/dw/smart_sales.db")
query = "SELECT sale_date, sale_amount FROM sale ORDER BY sale_date;"
data = pd.read_sql_query(query, connection)
connection.close()

# Step 2: Convert sale_date to datetime format
data['sale_date'] = pd.to_datetime(data['sale_date'])

# Step 3: Add a new column for just the month and year (e.g., '2024-01')
data['month'] = data['sale_date'].dt.to_period('M')

# Step 4: Group by the new 'month' column and sum sales
monthly_sales = data.groupby('month')['sale_amount'].sum().reset_index()

# Step 5: Convert 'month' back to a proper datetime for plotting
monthly_sales['month'] = monthly_sales['month'].dt.to_timestamp()

# Step 6: Plot the monthly sales
plt.figure(figsize=(10, 6))
plt.plot(monthly_sales['month'], monthly_sales['sale_amount'], marker='o', color='blue')
plt.title('Sales Growth Over Time', fontsize=16)
plt.xlabel('Month', fontsize=14)
plt.ylabel('Total Sales', fontsize=14)
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()