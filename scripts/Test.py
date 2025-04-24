import pandas as pd

# Adjust paths if needed
customer_df = pd.read_csv("data/customer.csv")
product_df = pd.read_csv("data/product.csv")
sales_df = pd.read_csv("data/sales.csv")

print("Customer Columns:", customer_df.columns.tolist())
print("Product Columns:", product_df.columns.tolist())
print("Sales Columns:", sales_df.columns.tolist())
