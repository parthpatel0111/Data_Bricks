import csv
import random
from datetime import datetime, timedelta

# Configuration
OUTPUT_FILE = r"D:\DATABRICKS_TRAINING\02_Source_Files\crm_customers.csv"
NUM_INITIAL_CUSTOMERS = 1000
NUM_UPDATES = 100
NUM_DELETES = 50

regions = ['North', 'South', 'East', 'West']
first_names = ['John', 'Jane', 'Alex', 'Emily', 'Chris', 'Katie', 'Michael', 'Sarah', 'David', 'Laura']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']

def generate_ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"

def generate_customer(customer_id, operation, base_date):
    fname = random.choice(first_names)
    lname = random.choice(last_names)
    return {
        "CustomerID": customer_id,
        "Name": f"{fname} {lname}",
        "Email": f"{fname.lower()}.{lname.lower()}{random.randint(1,99)}@example.com",
        "SSN": generate_ssn(),
        "Region": random.choice(regions),
        "Operation": operation,
        "ProcessDate": base_date.strftime("%Y-%m-%dT%H:%M:%S")
    }

def main():
    print("Generating CRM Data...")
    base_date = datetime.now() - timedelta(days=10)
    
    customers = []
    
    # 1. Generate Initial Inserts
    for i in range(1, NUM_INITIAL_CUSTOMERS + 1):
        customers.append(generate_customer(i, "INSERT", base_date))
        
    # 2. Generate Updates (SCD / CDC challenge)
    update_date = base_date + timedelta(days=2)
    for _ in range(NUM_UPDATES):
        cust = random.choice(customers[:NUM_INITIAL_CUSTOMERS])
        updated_cust = cust.copy()
        updated_cust["Region"] = random.choice(regions) # Simulate a customer moving regions
        updated_cust["Operation"] = "UPDATE"
        updated_cust["ProcessDate"] = update_date.strftime("%Y-%m-%dT%H:%M:%S")
        customers.append(updated_cust)
        
    # 3. Generate Deletes (GDPR / Right to be forgotten challenge)
    delete_date = base_date + timedelta(days=4)
    for _ in range(NUM_DELETES):
        cust = random.choice(customers[:NUM_INITIAL_CUSTOMERS])
        deleted_cust = cust.copy()
        deleted_cust["Operation"] = "DELETE"
        deleted_cust["ProcessDate"] = delete_date.strftime("%Y-%m-%dT%H:%M:%S")
        customers.append(deleted_cust)

    # Write to CSV
    with open(OUTPUT_FILE, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["CustomerID", "Name", "Email", "SSN", "Region", "Operation", "ProcessDate"])
        writer.writeheader()
        writer.writerows(customers)
        
    print(f"Success! {len(customers)} CRM records written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()