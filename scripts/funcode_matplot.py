import matplotlib.pyplot as plt

# Sample data
desserts = ['Cake', 'Pie', 'Cookie', 'Brownie']  # X-axis: Labels (strings)
sales = [20, 35, 15, 25]                         # Y-axis: Sales numbers (integers)

# Plotting the data
plt.figure(figsize=(8, 5))  # Set size of the chart
plt.plot(desserts, sales, marker='o', color='purple', label='Dessert Sales')

# Adding labels and title
plt.xlabel('Dessert Type')
plt.ylabel('Number Sold')
plt.title('Dessert Sales Report')
plt.grid(True)
plt.legend()
plt.tight_layout()

# Show the chart
plt.show()
