import csv
import random
from datetime import datetime, timedelta

OUTPUT_FILE = r"D:\DATABRICKS_TRAINING\02_Source_Files\erp_orders.csv"
NUM_ORDERS = 100000 # Large volume to trigger Spark UI metrics

def generate_orders():
    print("Generating ERP Orders Data (100,000 rows) with Data Skew...")
    base_date = datetime.now() - timedelta(days=30)
    
    orders = []
    for i in range(1, NUM_ORDERS + 1):
        # Challenge: Introduce massive Data Skew for ProductID
        # PROD_999 will appear 80% of the time. The other 50 products share the remaining 20%.
        if random.random() < 0.80:
            product_id = "PROD_999" # The skewed key
        else:
            product_id = f"PROD_{random.randint(1, 50)}"

        qty = random.randint(1, 10)
        amount = round(qty * random.uniform(10.0, 150.0), 2)
        
        # Distribute orders over the last 30 days for Liquid Clustering
        order_date = base_date + timedelta(days=random.randint(0, 30), hours=random.randint(0,23))

        orders.append({
            "OrderID": i,
            "CustomerID": random.randint(1, 1000), # Joins to CRM dataset
            "ProductID": product_id,
            "Quantity": qty,
            "Amount": amount,
            "OrderDate": order_date.strftime("%Y-%m-%d")
        })

    # Write to CSV
    with open(OUTPUT_FILE, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["OrderID", "CustomerID", "ProductID", "Quantity", "Amount", "OrderDate"])
        writer.writeheader()
        writer.writerows(orders)
        
    print(f"Success! {NUM_ORDERS} skewed ERP orders written to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_orders()