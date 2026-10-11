import json
import random
from datetime import datetime, timedelta

OUTPUT_FILE = r"D:\DATABRICKS_TRAINING\02_Source_Files\clickstream_logs.json"
NUM_EVENTS = 2500

event_types = ['page_view', 'add_to_cart', 'checkout']
pages = ['/home', '/product/123', '/product/456', '/cart', '/checkout']

def generate_event(event_id, base_date):
    event_type = random.choices(event_types, weights=[70, 20, 10])[0]
    
    # Base event structure
    event = {
        "event_id": f"evt_{event_id}",
        "customer_id": random.randint(1, 1000), # Joins to CRM dataset
        "event_type": event_type,
        "timestamp": (base_date + timedelta(seconds=random.randint(1, 86400))).strftime("%Y-%m-%dT%H:%M:%S"),
        "session_duration": random.randint(10, 300)
    }
    
    # Challenge 1: Inject bad data for the Quarantine Pattern
    if random.random() < 0.05: # 5% chance of a negative session duration
        event["session_duration"] = random.randint(-100, -1)
        
    # Challenge 2: Schema Evolution (Unexpected columns for _rescued_data)
    if random.random() < 0.02: # 2% chance of a random rogue column
        event["unexpected_promo_code"] = f"PROMO_{random.randint(10,99)}"

    # Challenge 3: Nested Arrays for 'add_to_cart'
    if event_type == 'add_to_cart':
        event["cart_items"] = [
            {"product_id": f"PROD_{random.randint(1,50)}", "qty": random.randint(1, 3)}
            for _ in range(random.randint(1, 4))
        ]
    else:
        event["page_url"] = random.choice(pages)

    return event

def main():
    print("Generating Clickstream JSON Data...")
    base_date = datetime.now() - timedelta(days=5)
    
    events = [generate_event(i, base_date) for i in range(1, NUM_EVENTS + 1)]
    
    # Write to JSON Lines format (standard for Big Data logs)
    with open(OUTPUT_FILE, mode='w') as file:
        for event in events:
            file.write(json.dumps(event) + "\n")
            
    print(f"Success! {NUM_EVENTS} JSON events written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()