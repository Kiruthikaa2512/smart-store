import pandas as pd
import numpy as np

# Load the dataset
sales_data = pd.read_csv('C:/Projects/smart-store-kiruthikaa/data/raw/sales_data.csv')

# Remove Duplicates
sales_data = sales_data.drop_duplicates(subset=['TransactionID'])

# Handling missing values
sales_data = sales_data.dropna()

# Remove the invalid values (BonusPoints >= 0)
sales_data = sales_data[sales_data['BonusPoints'] >= 0]

# Handle Outliers in SaleAmount using IQR
# Calculate Q1, Q3, and IQR
Q1 = sales_data['SaleAmount'].quantile(0.25)  # 25th percentile
Q3 = sales_data['SaleAmount'].quantile(0.75)  # 75th percentile
IQR = Q3 - Q1  # Interquartile Range

# Define lower and upper bounds for SaleAmount
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Keep rows within the bounds
sales_data = sales_data[(sales_data['SaleAmount'] >= lower_bound) & (sales_data['SaleAmount'] <= upper_bound)]

# Keep rows with valid PaymentType values
sales_data = sales_data[sales_data['PaymentType'].notnull()]  # Removes blank PaymentType rows
sales_data = sales_data[sales_data['PaymentType'].isin(['Cash', 'Credit', 'Debit'])]  # Keeps only valid PaymentType

# Saving the cleaned dataset
sales_data.to_csv('C:/Projects/smart-store-kiruthikaa/data/prepared/sales_data_prepared.csv', index=False)
print("Cleaned data saved to C:/Projects/smart-store-kiruthikaa/data/prepared/sales_data_prepared.csv")

# Check for duplicates in the sales dataset
print(f"Number of duplicate rows: {sales_data.duplicated().sum()}")
