import csv
import json
import random
import os
from datetime import datetime, timedelta

# Create output directory
output_dir = "source_data"
os.makedirs(output_dir, exist_ok=True)

# 1. Generate Customer Data (CSV - For SCD Type 2 Dimension)
customers = [
    {"CustomerID": 1001, "FirstName": "Emily", "LastName": "Chen", "City": "Seattle", "State": "WA", "CustomerSince": "2024-01-15", "UpdateTimestamp": "2026-10-01T08:00:00"},
    {"CustomerID": 1002, "FirstName": "Marcus", "LastName": "Johnson", "City": "Austin", "State": "TX", "CustomerSince": "2025-03-22", "UpdateTimestamp": "2026-10-01T08:00:00"},
    {"CustomerID": 1003, "FirstName": "Sophia", "LastName": "Martinez", "City": "Chicago", "State": "IL", "CustomerSince": "2025-11-05", "UpdateTimestamp": "2026-10-01T08:00:00"},
    {"CustomerID": 1004, "FirstName": "Liam", "LastName": "Patel", "City": "Boston", "State": "MA", "CustomerSince": "2026-02-10", "UpdateTimestamp": "2026-10-01T08:00:00"}
]

with open(f"{output_dir}/customers.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["CustomerID", "FirstName", "LastName", "City", "State", "CustomerSince", "UpdateTimestamp"])
    writer.writeheader()
    writer.writerows(customers)

# 2. Generate Product Data (JSON Lines - For Standard Dimension)
products = [
    {"ProductID": "PRD-901", "Category": "Electronics", "SubCategory": "Laptops", "Brand": "TechPro", "UnitPrice": 1299.99},
    {"ProductID": "PRD-902", "Category": "Electronics", "SubCategory": "Audio", "Brand": "SoundMax", "UnitPrice": 199.50},
    {"ProductID": "PRD-903", "Category": "Apparel", "SubCategory": "Outerwear", "Brand": "Summit", "UnitPrice": 89.99},
    {"ProductID": "PRD-904", "Category": "Apparel", "SubCategory": "Footwear", "Brand": "Stride", "UnitPrice": 120.00}
]

with open(f"{output_dir}/products.json", "w") as f:
    for p in products:
        f.write(json.dumps(p) + "\n")

# 3. Generate Sales Transactions (CSV - For Fact Table)
sales = []
start_date = datetime(2026, 10, 1)

for i in range(1, 201):  # Generate 200 orders
    order_time = start_date + timedelta(hours=random.randint(1, 100), minutes=random.randint(0, 59))
    sales.append({
        "OrderID": 50000 + i,
        "CustomerID": random.choice([1001, 1002, 1003, 1004]),
        "ProductID": random.choice(["PRD-901", "PRD-902", "PRD-903", "PRD-904"]),
        "TransactionDate": order_time.strftime("%Y-%m-%d %H:%M:%S"),
        "Quantity": random.randint(1, 4),
        "DiscountAmount": round(random.uniform(0, 15.0), 2)
    })

with open(f"{output_dir}/raw_sales.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["OrderID", "CustomerID", "ProductID", "TransactionDate", "Quantity", "DiscountAmount"])
    writer.writeheader()
    writer.writerows(sales)

print(f"Successfully generated retail data in the '{output_dir}' directory.")