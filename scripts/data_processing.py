import pandas as pd
import os

print("Current Working Directory:", os.getcwd())

# Read the customer data
customers_df = pd.read_csv('data/raw/customers_data.csv')
print(customers_df.head())

# Read the product data
products_df = pd.read_csv('data/raw/products_data.csv')
print(products_df.head())

# Read the sales data
sales_df = pd.read_csv('data/raw/sales_data.csv')
print(sales_df.head())
