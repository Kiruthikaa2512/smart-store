import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Connect to the database and get sales data
conn = sqlite3.connect("C:/Projects/smart-store-kiruthikaa/data/dw/smart_sales.db")
query = "SELECT sale_date, sale_amount FROM sale ORDER BY sale_date;"
data = pd.read_sql_query(query, conn)
conn.close()

# Step 2: Convert sale_date to date format
data['sale_date'] = pd.to_datetime(data['sale_date'])

# Step 3: Calculate average and standard deviation
average_sales = data['sale_amount'].mean()
standard_deviation = data['sale_amount'].std()

# Optional: Print stats to understand thresholds
print("Average Sale Amount:", round(average_sales, 2))
print("Standard Deviation:", round(standard_deviation, 2))

# Adjust sensitivity of anomaly detection (easier to catch Low/High)
low_threshold = average_sales - 0.5 * standard_deviation
high_threshold = average_sales + 0.5 * standard_deviation
print("Low Threshold:", round(low_threshold, 2))
print("High Threshold:", round(high_threshold, 2))

# Step 4: Label anomalies based on thresholds
def is_anomaly(sale):
    if sale > high_threshold:
        return 'High'
    elif sale < low_threshold:
        return 'Low'
    else:
        return 'Normal'

data['anomaly'] = data['sale_amount'].apply(is_anomaly)

# Step 5: Print some info to check anomaly counts
print("Number of High anomalies:", len(data[data['anomaly'] == 'High']))
print("Number of Low anomalies:", len(data[data['anomaly'] == 'Low']))
print("Lowest 10 sale amounts:")
print(data['sale_amount'].sort_values().head(10))

# Step 6: Plot the results
plt.figure(figsize=(10, 6))

# Plot all sales
plt.plot(data['sale_date'], data['sale_amount'], label='Daily Sales', color='blue')

# Plot high anomalies in red
high_anomalies = data[data['anomaly'] == 'High']
plt.scatter(high_anomalies['sale_date'], high_anomalies['sale_amount'], color='red', label='High Anomaly', zorder=5)

# Plot low anomalies in green
low_anomalies = data[data['anomaly'] == 'Low']
plt.scatter(low_anomalies['sale_date'], low_anomalies['sale_amount'], color='green', label='Low Anomaly', zorder=5)

# Final plot formatting
plt.title("Sales with Anomalies")
plt.xlabel("Date")
plt.ylabel("Sale Amount")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
