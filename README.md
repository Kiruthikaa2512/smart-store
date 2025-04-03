# Project Title
Smart Store Project - Kiruthikaa

## Table of Contents
- Introduction
- Project Goals
- Technologies Used
- Features
- Folder Structure
- Setup Instructions
- Commands & Workflow
- Acknowledgements
- Future Enhancements

## Introduction
This project focuses on building a smart store solution using modern tools and techniques. The implementation includes logging functionality, Python scripting, and data preparation.

## Project Goals
- Analyze customer and sales data.
- Build predictive models for forecasting.

## Technologies Used
- Python
- Pandas
- Jupyter Notebook

## Features
Project initialization with Git and virtual environment.
Centralized logging utility (logger.py) for tracking script execution.
Python scripts for data preparation and workflow automation focuses mainly on the below features. 
- **Efficient Inventory Tracking**: Manage stock levels with ease.
- **User-Friendly Interface**: Automated and easy to use.
- **Scalable Design**: Versatile to suit all business size. 

## Folder Structure
```
smart-store-kiruthikaa/
│
├── README.md              # Project documentation  
├── data/                  # Placeholder for datasets  
├── notebooks/             # Jupyter notebooks  
├── scripts/               # Python scripts like data_prep.py  
├── utils/                 # Utility scripts like logger.py  
└── .gitignore             # Git ignore file  
```

## Setup Instructions - Week 1 & Week 2
1. Clone the repository.
2. Navigate to the project folder . 
3. Create a virtual environment
4. Activate the virtual environment
5. Install dependencies using `pip install -r requirements.txt`.

```bash
# Clone the repository
git clone https://github.com/Kiruthikaa2512/smart-store.git

# Navigate to the project folder
cd smart-store-kiruthikaa

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment (Windows)
.\.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```
## Data Collection, Cleaning, & ETL/ELT - Week 3
## Data Cleaning Process and Commands 

The data cleaning pipeline now integrates a modular class-based approach using the `DataScrubber` class. This addition streamlines data preparation tasks while ensuring maintainability and reusability across different datasets. Below is a detailed outline:

### Data Scrubber Class

The `DataScrubber` class, implemented in the project, encapsulates various data cleaning functionalities and provides a unified interface for handling datasets. It is designed to be flexible and scalable for diverse cleaning needs.

### Key Features of `DataScrubber`

- **Column Validation**: Ensures required columns are present before processing.
- **Duplicate Removal**: Removes redundant rows to improve data integrity.
- **Data Standardization**: Handles typos, formats dates, and standardizes numeric fields.
- **Outlier Detection and Removal**: Identifies and removes data anomalies.
- **Missing Value Handling**: Imputes or removes missing values based on context.
- **Customizable Rules**: Allows defining dataset-specific cleaning rules.

### Example Workflow Using `DataScrubber`

1. **Initialization**: The class is initialized with dataset-specific configurations.
2. **Execution**: Methods are called to perform step-by-step cleaning.
3. **Output**: Cleaned data is returned or saved as needed.

### Example Script Integration

The `DataScrubber` class is utilized in the individual preparation scripts:

1. **`prepare_customers_data.py`**:
   - Removes duplicate customers.
   - Standardizes names and formats `JoinDate`.
   - Aggregates loyalty points.

   Example Usage:
   ```python
   from scripts.data_scrubber import DataScrubber

   scrubber = DataScrubber(custom_rules={"customerid": "mandatory"})
   cleaned_data = scrubber.clean(customers_data)

### Core Scripts in the Data Scrubbing Workflow
The `data_prep.py` script is responsible for cleaning and preprocessing datasets for the Smart Store Project. Below is a detailed outline of the commands used:

#### 1. Read Raw Data
Reads raw CSV files and standardizes column names to ensure consistency:

df = pd.read_csv(file_path)  # Load the CSV file
df.columns = df.columns.str.strip().str.lower()  # Standardize column names

### 2. Remove duplicate Rows
df = df.drop_duplicates()  # Remove redundant rows

### 3. Handle Missing Rows
df = df.drop_duplicates()  # Remove redundant rows
df = df.dropna(subset=["customerid", "saledate"])  # Remove invalid rows

### 4. Correct Typos and Standardize Values
df['paymenttype'] = df['paymenttype'].str.capitalize().replace({"Dbit": "Debit"})
Fixes supplier typos (e.g., "Blooom" becomes "Bloom"):
df['supplier'] = df['supplier'].replace({"Blooom": "Bloom"})
Replaces invalid names in customers_data.csv
df['name'] = df['name'].str.strip().replace({"Xys": "Anonymous", "Unknown": "Anonymous"}).fillna("Anonymous")

### 5.Standardize Date Formats
df['saledate'] = pd.to_datetime(df['saledate'], errors='coerce').dt.strftime('%Y-%m-%d')
df = df.dropna(subset=['saledate'])  # Remove rows with invalid dates

### 6.Filter Outliers
For SaleAmount
df = df[(df['saleamount'] > 0) & (df['saleamount'] <= 8000)]  # Valid range
For StockQuantity
df['stockquantity'] = pd.to_numeric(df['stockquantity'], errors='coerce').fillna(0)
df = df.loc[(df['stockquantity'] >= 0) & (df['stockquantity'] <= 1500)]  # Excludes outliers
Ensure BonusPoints
df.loc[df['bonuspoints'] < 0, 'bonuspoints'] = 0

### 7.Aggregate Data
Groups and aggregates customer data to consolidate duplicate entries:
df = df.groupby('name', as_index=False).agg({
    'customerid': 'first',
    'loyaltypoints': 'sum',
    'joindate': 'first'
})

### 8.Save Cleaned Data
Saves cleaned datasets to the data/prepared directory:
df.to_csv(file_path, index=False)  # Save cleaned CSV

### Workflow commands for data cleaning process
# Execute the data cleaning script
python scripts/data_prep.py

# View logs to track data cleaning progress
tail -f utils/logger.log

### Key Issues Confirmed in This Script
Here are the exact problems we encountered in the data_prep.py script initially:

1. Missing Required Columns in products_data.csv
Error:

plaintext
Copy
Edit
KeyError: "Missing required columns in products_data.csv: {'price'}"
Fix:

Added a check before processing to validate required columns ('price', 'ProductName', etc.).

Converted column names to lowercase before checking.

2. Column Name Inconsistencies
Issue: Columns had leading/trailing spaces or inconsistent casing ("Price" vs. "price").

Fix: Used

python
Copy
Edit
df.columns = df.columns.str.strip().str.lower()
to standardize all column names.

3. Duplicate Data
Issue: Some datasets contained duplicate rows.
Fix: Used df.drop_duplicates() to remove them.

4. Missing Critical Data in customers_data.csv
Issue: CustomerID or Name was missing in some rows.
Fix: Dropped rows with missing values in these columns.

5. Date Formatting Issues in sales_data.csv
Issue: SaleDate had inconsistent formats and invalid values.
Fix: Used

## Commands & Workflow
```bash
```
# Data Warehousing, Star Schemas, & Decision Support - Week 4

## **Data Warehousing & ETL Pipeline**

#### **Overview**
As part of the Smart Store project, this week we implemented a **Data Warehouse** using a **star schema** to enable efficient data analysis and decision support. The ETL pipeline was designed to extract, transform, and load data into the database, ensuring high-quality data ingestion for actionable insights.

#### **Star Schema Design**
The star schema consists of:
- **Fact Table (`sale`)**:
  - Central table capturing transactional data like `sale_amount`, `sale_date`, and `campaignid`.
  - Linked to dimension tables using foreign keys (`customer_id`, `product_id`).
- **Dimension Tables**:
  - **`customer`**: Attributes such as `name`, `region`, and `loyaltypoints`.
  - **`product`**: Product-specific details including `product_name`, `category`, and `unitprice`.

**Schema Representation**:
```
                customer
                   ↑
                   |
    product ←— sale → store, campaign
```
#### **ETL Pipeline Process**
- **Data Preprocessing**:
  - Corrected IDs to ensure uniqueness and consistency.
  - Handled missing values by replacing NaN with logical defaults (e.g., `0` for numbers, `"Unknown"` for text).
  - Deduplicated rows to maintain data integrity.
- **Schema Creation**:
  - Generated tables (`customer`, `product`, `sale`) in SQLite based on the star schema.
- **Data Loading**:
  - Ingested cleaned data from CSV files into the database for downstream analysis.
- 
#### **Steps to Execute**
1. Ensure cleaned CSV files are placed in `data/prepared/`:
   - `customers_data_prepared.csv`
   - `products_data_prepared.csv`
   - `sales_data_prepared.csv`
2. Run the ETL pipeline:
   ```bash
   python scripts/etl_to_dw.py
   ```
3. Verify the data and relationships using SQL queries:
   ```sql
   SELECT * FROM customer LIMIT 5;
   SELECT * FROM product LIMIT 5;
   SELECT * FROM sale LIMIT 5;

   SELECT s.sale_id, c.name, p.product_name, s.sale_amount
   FROM sale s
   JOIN customer c ON s.customer_id = c.customer_id
   JOIN product p ON s.product_id = p.product_id
   LIMIT 5;
4. Output should appear as show below for each table:
  ![Customer Table](images/Customer_Table.png)
  ![Products Table](images/Products_Table.png)
  ![Sales Table](images/Sales_Table.png)

   ```
#### **Challenges Encountered**
- **Column Mapping Issues**:
  - Solution: Explicitly mapped CSV headers (`transactionid`, `productid`, etc.) to schema fields (`sale_id`, `product_id`).
- **Duplicate Data**:
  - Solution: Implemented deduplication logic for primary keys.
- **Data Gaps**:
  - Solution: Handled missing values by filling logical defaults during preprocessing.

# Initialize Git Repository
git init  
git remote add origin https://github.com/your-repo-link.git  

# Initialize the Git Repository
git init  
git remote add origin https://github.com/your-repo-link.git  

#Add Logger Script:
mkdir utils  
touch utils/logger.py  
# Copy the content from the starter repository into logger.py  

#Add Python Script
mkdir scripts  
touch scripts/data_prep.py  
# Copy the content from the starter repository into data_prep.py  

# Run the Python Script
python scripts/data_prep.py  

#Push updates to Remote Repository
git add .  
git commit -m "Updated README and added logger script"  
git push origin main  

## Acknowledgements

Special Thanks To:
Dr.Case for her guidance and support
Open Source Contributors for essential license and tools. 

## Future Enhancements
Comprehensive Reporting for Sales, Inventory, Customer Management. 
Seamless integration with leading e-commerce platforms. 