import os
import pandas as pd

SOURCE_DIR = r"D:\DATABRICKS_TRAINING\02_Source_Files"
# Create subfolders for the streaming simulation
os.makedirs(os.path.join(SOURCE_DIR, "crm_daily"), exist_ok=True)
os.makedirs(os.path.join(SOURCE_DIR, "clickstream_daily"), exist_ok=True)
os.makedirs(os.path.join(SOURCE_DIR, "erp_daily"), exist_ok=True)

def split_csv(filename, output_folder, chunk_size, prefix):
    print(f"Splitting {filename}...")
    filepath = os.path.join(SOURCE_DIR, filename)
    try:
        df = pd.read_csv(filepath)
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
        for i in range(num_chunks):
            chunk = df[i*chunk_size : (i+1)*chunk_size]
            chunk.to_csv(os.path.join(SOURCE_DIR, output_folder, f"{prefix}_day{i+1}.csv"), index=False)
        print(f"  -> Created {num_chunks} daily files in {output_folder}")
    except Exception as e:
        print(f"Error reading {filename}: {e}")

def split_json(filename, output_folder, chunk_size, prefix):
    print(f"Splitting {filename}...")
    filepath = os.path.join(SOURCE_DIR, filename)
    try:
        df = pd.read_json(filepath, lines=True)
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
        for i in range(num_chunks):
            chunk = df[i*chunk_size : (i+1)*chunk_size]
            chunk.to_json(os.path.join(SOURCE_DIR, output_folder, f"{prefix}_day{i+1}.json"), orient='records', lines=True)
        print(f"  -> Created {num_chunks} daily files in {output_folder}")
    except Exception as e:
        print(f"Error reading {filename}: {e}")

if __name__ == "__main__":
    split_csv("crm_customers.csv", "crm_daily", 300, "crm")
    split_json("clickstream_logs.json", "clickstream_daily", 500, "clicks")
    split_csv("erp_orders.csv", "erp_daily", 20000, "erp")
    print("Splitting complete! The interns can now drop these files in day by day.")