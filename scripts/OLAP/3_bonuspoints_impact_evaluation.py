import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Connect to the database and load relevant data
conn = sqlite3.connect("C:/Projects/smart-store-kiruthikaa/data/dw/smart_sales.db")
query = """
SELECT 
    sale_id,
    sale_amount,
    bonuspoints
FROM sale;
"""
df = pd.read_sql_query(query, conn)
conn.close()

# Step 2: Fill empty bonuspoints with 0 (in case there are any)
df['bonuspoints'] = df['bonuspoints'].fillna(0)

# Step 3: Create bonus points range buckets
def categorize_bonus(points):
    if points < 100:
        return "0-99"
    elif points < 200:
        return "100-199"
    elif points < 300:
        return "200-299"
    else:
        return "300+"

df['bonus_range'] = df['bonuspoints'].apply(categorize_bonus)

# Step 4: Group by bonus range and calculate average sales
summary = df.groupby('bonus_range')['sale_amount'].mean().reset_index()

# Step 5: Plot the result
plt.figure(figsize=(8, 5))
plt.bar(summary['bonus_range'], summary['sale_amount'], color='green')
plt.title('Average Sales by Bonus Points Range', fontsize=14)
plt.xlabel('Bonus Points Range', fontsize=12)
plt.ylabel('Average Sale Amount', fontsize=12)
plt.grid(axis='y')
plt.tight_layout()
plt.show()
