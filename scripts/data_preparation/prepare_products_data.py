import pandas as pd
products_data = pd.read_csv('C:/Projects/smart-store-kiruthikaa/data/raw/products_data.csv')

# Fix typos in Supplier
products_data['Supplier'] = products_data['Supplier'].replace({'Blooom': 'Bloom'})

# Remove duplicates based on relevant columns, ignoring 'ProductID'
products_data = products_data.drop_duplicates(subset=['ProductName', 'Category', 'UnitPrice', 'StockQuantity', 'Supplier'])

#Handling missing values
products_data = products_data.dropna()

#Remove the invalid values
products_data = products_data[products_data['StockQuantity'] >= 0]

# Remove rows where StockQuantity exceeds a specific threshold (e.g., 1500)
products_data = products_data[products_data['StockQuantity'] <= 1500]

#Saving the cleaned dataset
products_data.to_csv('C:/Projects/smart-store-kiruthikaa/data/prepared/products_data_prepared.csv', index=False)
print(f"Cleaned data saved to C:/Projects/smart-store-kiruthikaa/data/prepared/products_data_prepared.csv")