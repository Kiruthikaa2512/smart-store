import pandas as pd

#Clean Customer Data
customers_data = pd.read_csv('C:/Projects/smart-store-kiruthikaa/data/raw/customers_data.csv')

#Remove Duplicates
customers_data = customers_data.drop_duplicates(subset=['CustomerID', 'Name'])

#Handling missing values
customers_data = customers_data.dropna()

#Remove the invalid values
customers_data = customers_data[customers_data['LoyaltyPoints'] >= 0]

# Remove rows with 'Unknown' in the Name column
customers_data = customers_data[customers_data['Name'] != 'Unknown']

#Saving the cleaned dataset
customers_data.to_csv('C:/Projects/smart-store-kiruthikaa/data/prepared/customers_data_prepared.csv', index=False)
print(f"Cleaned data saved to C:/Projects/smart-store-kiruthikaa/data/prepared/customers_data_prepared.csv")

#Reloading the file
cleaned_data = pd.read_csv('C:/Projects/smart-store-kiruthikaa/data/prepared/customers_data_prepared.csv')

# Check for duplicates in the sales dataset
print(f"Number of duplicate rows: {customers_data.duplicated().sum()}")