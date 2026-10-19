import json
import random
from datetime import datetime, date, timedelta
from decimal import Decimal
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, DateType, TimestampType, DecimalType
)

# Set seed for reproducible enterprise testing
random.seed(42)

CATALOG = "retail_mfg_dev"
BASE_VOLUME_PATH = f"/Volumes/{CATALOG}/landing_volume/source_files"
NOW_TS = datetime(2026, 10, 14, 9, 0, 0)

print(f"Starting Clean Day 1 Baseline Data Generation for {CATALOG}...")

# ==============================================================================
# PART 0: CLEAN RESET (PREVENTS DUPLICATE FILES & DUPLICATE CDF COMMIT VERSIONS)
# ==============================================================================
bronze_tables = [
    "raw_erp_customers", "raw_hr_employees", "raw_erp_suppliers",
    "raw_wms_warehouses", "raw_retail_stores", "raw_wms_inventory"
]
for tbl in bronze_tables:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze.{tbl}")

volume_folders = [
    "online_sales_stream", "pos_sales_stream", "iot_telemetry_stream",
    "promotions_csv", "product_catalog_parquet"
]
for folder in volume_folders:
    folder_path = f"{BASE_VOLUME_PATH}/{folder}"
    dbutils.fs.rm(folder_path, recurse=True)
    dbutils.fs.mkdirs(folder_path)

print("0/11: Reset Bronze tables and Volume directories for a clean Version 0 load.")

# ==============================================================================
# PART 1: POPULATE THE 6 BRONZE OPERATIONAL CDF TABLES
# ==============================================================================

first_names = ["Aarav", "Emily", "Liam", "Sophia", "Noah", "Olivia", "Vikram", "Priya", "Lucas", "Mia"]
last_names = ["Patel", "Chen", "Smith", "Garcia", "Kim", "Muller", "Sharma", "Johnson", "Silva", "Brown"]
cities = [("Seattle", "US"), ("Mumbai", "IN"), ("London", "GB"), ("Berlin", "DE"), ("Toronto", "CA"), ("Tokyo", "JP")]
tiers = ["Standard", "Silver", "Gold", "Platinum"]
regions = ["AMER", "EMEA", "APAC"]

# 1. raw_erp_customers (1,000 unique rows: CUST-0001 to CUST-1000)
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("_updated_at", TimestampType(), True)
])

customers_data = []
for i in range(1, 1001):
    fn = first_names[i % len(first_names)]
    ln = last_names[(i // len(first_names)) % len(last_names)]
    city, country = cities[i % len(cities)]
    customers_data.append((
        f"CUST-{i:04d}",
        fn,
        ln,
        f"{fn.lower()}.{ln.lower()}.{i:04d}@example.com",
        f"+1-555-{(100 + (i // 100)):03d}-{(1000 + i):04d}",
        date(1965 + (i % 40), (i % 12) + 1, (i % 28) + 1),
        tiers[i % len(tiers)],
        city,
        country,
        NOW_TS
    ))

df_customers = spark.createDataFrame(customers_data, schema=customers_schema).dropDuplicates(["customer_id"])
(df_customers.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_erp_customers"))
print("1/11: Loaded 1,000 unique rows into bronze.raw_erp_customers")

# 2. raw_hr_employees (150 unique rows: EMP-001 to EMP-150)
departments = ["Sales", "Warehouse", "Production", "Finance", "Operations"]
titles = ["Associate", "Specialist", "Supervisor", "Manager", "Director"]

employees_schema = StructType([
    StructField("employee_id", StringType(), False),
    StructField("full_name", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("department", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("employment_status", StringType(), True),
    StructField("manager_employee_id", StringType(), True),
    StructField("_updated_at", TimestampType(), True)
])

employees_data = []
for i in range(1, 151):
    fn = first_names[i % len(first_names)]
    ln = last_names[(i // len(first_names)) % len(last_names)]
    employees_data.append((
        f"EMP-{i:03d}",
        f"{fn} {ln} #{i:03d}",
        titles[i % len(titles)],
        departments[i % len(departments)],
        regions[i % 3],
        date(2018 + (i % 8), (i % 12) + 1, (i % 28) + 1),
        True,
        "Active",
        f"EMP-{max(1, (i // 10)):03d}",
        NOW_TS
    ))

df_employees = spark.createDataFrame(employees_data, schema=employees_schema).dropDuplicates(["employee_id"])
(df_employees.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_hr_employees"))
print("2/11: Loaded 150 unique rows into bronze.raw_hr_employees")

# 3. raw_erp_suppliers (50 unique rows: SUP-001 to SUP-050)
sup_categories = ["Raw Materials", "Packaging", "Finished Goods", "Logistics"]
pay_terms = ["NET30", "NET45", "NET60"]
risk_ratings = ["Low", "Medium", "High"]

suppliers_schema = StructType([
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("supplier_category", StringType(), True),
    StructField("contract_tier", StringType(), True),
    StructField("payment_terms", StringType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("risk_rating", StringType(), True),
    StructField("last_audit_date", DateType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("_updated_at", TimestampType(), True)
])

suppliers_data = []
for i in range(1, 51):
    city, country = cities[i % len(cities)]
    suppliers_data.append((
        f"SUP-{i:03d}",
        f"Global Vendor {i:03d} Corp",
        f"{first_names[i % len(first_names)]} {last_names[i % len(last_names)]}",
        sup_categories[i % len(sup_categories)],
        ["Tier 1", "Tier 2", "Strategic"][i % 3],
        pay_terms[i % len(pay_terms)],
        int(5 + (i % 40)),
        country,
        risk_ratings[i % len(risk_ratings)],
        date(2026, (i % 9) + 1, (i % 28) + 1),
        True,
        NOW_TS
    ))

df_suppliers = spark.createDataFrame(suppliers_data, schema=suppliers_schema).dropDuplicates(["supplier_id"])
(df_suppliers.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_erp_suppliers"))
print("3/11: Loaded 50 unique rows into bronze.raw_erp_suppliers")

# 4. raw_wms_warehouses (10 unique rows: WH-001 to WH-010)
warehouses_schema = StructType([
    StructField("warehouse_id", StringType(), False),
    StructField("facility_name", StringType(), True),
    StructField("facility_type", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("capacity_pallets", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("manager_employee_id", StringType(), True),
    StructField("_updated_at", TimestampType(), True)
])

warehouses_data = []
for i in range(1, 11):
    city, country = cities[i % len(cities)]
    fac_type = "Distribution Center" if i <= 5 else "FACTORY"
    warehouses_data.append((
        f"WH-{i:03d}",
        f"{city} {fac_type} #{i:02d}",
        fac_type,
        f"{100 * i} Industrial Pkwy",
        city,
        f"State-{country}",
        country,
        regions[i % 3],
        int(5000 + (i * 1500)),
        True,
        f"EMP-{i:03d}",
        NOW_TS
    ))

df_warehouses = spark.createDataFrame(warehouses_data, schema=warehouses_schema).dropDuplicates(["warehouse_id"])
(df_warehouses.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_wms_warehouses"))
print("4/11: Loaded 10 unique rows into bronze.raw_wms_warehouses")

# 5. raw_retail_stores (30 unique rows: STR-001 to STR-030)
store_types = ["Flagship", "Standard", "Outlet", "Kiosk"]

stores_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("square_footage", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("manager_name", StringType(), True),
    StructField("open_date", DateType(), True),
    StructField("store_status", StringType(), True),
    StructField("_updated_at", TimestampType(), True)
])

stores_data = []
for i in range(1, 31):
    city, country = cities[i % len(cities)]
    stores_data.append((
        f"STR-{i:03d}",
        f"{city} Retail Store #{i:02d}",
        store_types[i % len(store_types)],
        int(3000 + (i * 1000)),
        city,
        country,
        regions[i % 3],
        f"{first_names[i % len(first_names)]} {last_names[i % len(last_names)]}",
        date(2015 + (i % 10), (i % 12) + 1, 1),
        "Open",
        NOW_TS
    ))

df_stores = spark.createDataFrame(stores_data, schema=stores_schema).dropDuplicates(["store_id"])
(df_stores.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_retail_stores"))
print("5/11: Loaded 30 unique rows into bronze.raw_retail_stores")

# 6. raw_wms_inventory (1,500 unique rows: ITXN-00001 to ITXN-01500)
txn_types = ["Receive", "Pick", "Transfer", "Adjustment"]

inventory_schema = StructType([
    StructField("inventory_txn_id", StringType(), False),
    StructField("warehouse_id", StringType(), True),
    StructField("destination_store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("txn_type", StringType(), True),
    StructField("quantity_changed", IntegerType(), True),
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("unit_of_measure", StringType(), True),
    StructField("bin_location", StringType(), True),
    StructField("lot_number", StringType(), True),
    StructField("expiration_date", DateType(), True),
    StructField("operator_id", StringType(), True),
    StructField("reference_doc_id", StringType(), True),
    StructField("cost_basis", DecimalType(10, 2), True),
    StructField("is_reconciliation", BooleanType(), True),
    StructField("source_system", StringType(), True),
    StructField("_updated_at", TimestampType(), True)
])

inventory_data = []
for i in range(1, 1501):
    qty_chg = random.randint(-50, 200)
    if qty_chg == 0:
        qty_chg = 25
    unit_c = round(random.uniform(10.0, 250.0), 2)
    inventory_data.append((
        f"ITXN-{i:05d}",
        f"WH-{((i - 1) % 10) + 1:03d}",
        f"STR-{((i - 1) % 30) + 1:03d}",
        f"PROD-{((i - 1) % 200) + 1:03d}",
        f"SUP-{((i - 1) % 50) + 1:03d}",
        txn_types[i % len(txn_types)],
        int(qty_chg),
        int(100 + (i % 4900)),
        "EA",
        f"AISLE-{(i % 20) + 1:02d}-BIN-{(i % 50) + 1:02d}",
        f"LOT-2026-{i:05d}",
        date(2027, (i % 12) + 1, 28),
        f"EMP-{((i - 1) % 150) + 1:03d}",
        f"PO-2026-{i:05d}",
        Decimal(f"{unit_c:.2f}"),
        (i % 25 == 0),
        "WMS_CORE_V2",
        NOW_TS - timedelta(minutes=i)
    ))

df_inventory = spark.createDataFrame(inventory_data, schema=inventory_schema).dropDuplicates(["inventory_txn_id"])
(df_inventory.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_wms_inventory"))
print("6/11: Loaded 1,500 unique rows into bronze.raw_wms_inventory")

# ==============================================================================
# PART 2: POPULATE THE 5 LANDING VOLUME FOLDERS (PARQUET, CSV, MULTI-FILE JSON)
# ==============================================================================

# 7. product_catalog_parquet (200 unique rows: PROD-001 to PROD-200)
categories = [("Electronics", "Audio"), ("Electronics", "Computers"), ("Apparel", "Outerwear"), ("Home", "Kitchen")]
brands = ["ApexTech", "NovaGear", "EcoWear", "PrimeHome", "VoltEdge"]

products_schema = StructType([
    StructField("sku", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("unit_cost", DecimalType(10, 2), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("weight_kg", DoubleType(), True),
    StructField("lifecycle_status", StringType(), True),
    StructField("_updated_at", TimestampType(), True)
])

products_data = []
for i in range(1, 201):
    cat1, cat2 = categories[i % len(categories)]
    brand = brands[i % len(brands)]
    cost = round(15.0 + (i * 1.75), 2)
    price = round(cost * 1.65, 2)
    products_data.append((
        f"PROD-{i:03d}",
        f"{brand} {cat2} Model #{i:03d}",
        cat1,
        cat2,
        brand,
        "Finished Good",
        Decimal(f"{cost:.2f}"),
        Decimal(f"{price:.2f}"),
        float(round(0.5 + (i % 15) * 0.8, 2)),
        "Active",
        NOW_TS
    ))

df_products = spark.createDataFrame(products_data, schema=products_schema).dropDuplicates(["sku"])
df_products.coalesce(1).write.mode("overwrite").parquet(f"{BASE_VOLUME_PATH}/product_catalog_parquet")
print("7/11: Wrote 200 unique rows to product_catalog_parquet/")

# 8. promotions_csv (25 unique rows: PROMO-01 to PROMO-25)
promo_csv_path = f"{BASE_VOLUME_PATH}/promotions_csv/promotions_day1.csv"
promo_types = ["Percentage", "Fixed Amount", "BOGO", "Loyalty"]
channels = ["Online", "In-store", "Omni-channel"]

with open(promo_csv_path, "w", encoding="utf-8") as f:
    f.write("promo_code,campaign_name,discount_type,discount_value,start_date,end_date,channel,budget_allocated,is_active,_updated_at\n")
    for i in range(1, 26):
        p_type = promo_types[i % len(promo_types)]
        d_val = round(5.0 + (i * 1.25), 2)
        budget = round(10000.0 + (i * 5000.0), 2)
        f.write(f"PROMO-{i:02d},Campaign_2026_Q{(i%4)+1}_{i:02d},{p_type},{d_val},2026-01-01,2026-12-31,{channels[i % len(channels)]},{budget},true,{NOW_TS.isoformat()}\n")
print("8/11: Wrote 25 unique rows to promotions_csv/promotions_day1.csv")

# 9. online_sales_stream (1,500 unique rows: ORD-2026-00001 to ORD-2026-01500 across 5 JSON files)
for batch in range(1, 6):
    file_path = f"{BASE_VOLUME_PATH}/online_sales_stream/online_orders_day1_part{batch:02d}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        for j in range(1, 301):
            idx = (batch - 1) * 300 + j
            qty = (idx % 5) + 1
            u_price = round(25.0 + (idx % 150) * 2.5, 2)
            disc = round(u_price * qty * (0.05 if idx % 3 == 0 else 0.0), 2)
            tax = round((u_price * qty - disc) * 0.08, 2)
            ship = round(5.0 + (idx % 10), 2)
            tot = round((u_price * qty) - disc + tax + ship, 2)
            event_dt = NOW_TS - timedelta(minutes=idx * 2)
            record = {
                "order_id": f"ORD-2026-{idx:05d}",
                "order_line_number": 1,
                "order_ts": event_dt.isoformat(),
                "order_date_key": int(event_dt.strftime("%Y%m%d")),
                "customer_id": f"CUST-{((idx - 1) % 1000) + 1:04d}",
                "sku": f"PROD-{((idx - 1) % 200) + 1:03d}",
                "promo_code": f"PROMO-{((idx - 1) % 25) + 1:02d}",
                "warehouse_id": f"WH-{((idx - 1) % 5) + 1:03d}",
                "sales_channel": ["Web", "Mobile App", "Marketplace"][idx % 3],
                "quantity": qty,
                "unit_price": u_price,
                "discount_amount": disc,
                "tax_amount": tax,
                "shipping_amount": ship,
                "total_amount": tot,
                "currency_code": "USD",
                "payment_method": ["Credit Card", "PayPal", "Apple Pay"][idx % 3],
                "shipping_status": ["Pending", "Shipped", "Delivered"][idx % 3],
                "device_type": ["iOS", "Android", "Desktop"][idx % 3],
                "session_id": f"SESS-2026-{idx:06d}"
            }
            if idx % 20 == 0:
                record["unmapped_browser_engine"] = "Blink_v128"
            f.write(json.dumps(record) + "\n")
print("9/11: Wrote 1,500 unique rows across 5 JSON files in online_sales_stream/")

# 10. pos_sales_stream (1,500 unique rows: POS-2026-00001 to POS-2026-01500 across 5 JSON files)
for batch in range(1, 6):
    file_path = f"{BASE_VOLUME_PATH}/pos_sales_stream/pos_txns_day1_part{batch:02d}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        for j in range(1, 301):
            idx = (batch - 1) * 300 + j
            is_bad = (j <= 10)
            qty = 0 if (is_bad and j <= 5) else ((idx % 6) + 1)
            u_price = -25.00 if (is_bad and j > 5) else round(15.0 + (idx % 120) * 2.0, 2)
            disc = 0.0 if is_bad else round(u_price * qty * 0.05, 2)
            tax = 0.0 if is_bad else round((u_price * qty - disc) * 0.07, 2)
            tot = round((u_price * qty) - disc + tax, 2)
            event_dt = NOW_TS - timedelta(minutes=idx * 2)
            record = {
                "receipt_id": f"POS-2026-{idx:05d}",
                "line_number": 1,
                "sale_ts": event_dt.isoformat(),
                "sale_date_key": int(event_dt.strftime("%Y%m%d")),
                "store_id": f"STR-{((idx - 1) % 30) + 1:03d}",
                "employee_id": f"EMP-{((idx - 1) % 150) + 1:03d}",
                "customer_id": f"CUST-{((idx - 1) % 1000) + 1:04d}",
                "sku": f"PROD-{((idx - 1) % 200) + 1:03d}",
                "promo_code": f"PROMO-{((idx - 1) % 25) + 1:02d}",
                "quantity": qty,
                "unit_price": u_price,
                "discount_amount": disc,
                "tax_amount": tax,
                "total_amount": tot,
                "register_id": f"REG-{(idx % 12) + 1:02d}",
                "transaction_type": "Sale",
                "loyalty_used_flag": (idx % 2 == 0),
                "payment_type": ["Card", "Cash", "Mobile Wallet"][idx % 3],
                "shift_id": ["MORNING", "AFTERNOON", "EVENING"][idx % 3]
            }
            f.write(json.dumps(record) + "\n")
print("10/11: Wrote 1,500 unique rows (1,450 clean + 50 quarantine rows) across 5 JSON files in pos_sales_stream/")

# 11. iot_telemetry_stream (2,000 unique rows: IOT-2026-00001 to IOT-2026-02000 across 5 JSON files)
for batch in range(1, 6):
    file_path = f"{BASE_VOLUME_PATH}/iot_telemetry_stream/iot_sensors_day1_part{batch:02d}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        for j in range(1, 401):
            idx = (batch - 1) * 400 + j
            event_dt = NOW_TS - timedelta(minutes=idx)
            record = {
                "event_id": f"IOT-2026-{idx:05d}",
                "event_date_key": int(event_dt.strftime("%Y%m%d")),
                "event_timestamp": event_dt.isoformat(),
                "facility_id": f"WH-{6 + (idx % 5):03d}",
                "sku": f"PROD-{((idx - 1) % 200) + 1:03d}",
                "operator_employee_id": f"EMP-{((idx - 1) % 150) + 1:03d}",
                "device_id": f"SENSOR-{(idx % 50) + 1:03d}",
                "machine_id": f"CNC-MACH-{(idx % 20) + 1:02d}",
                "production_line_id": f"LINE-{(idx % 5) + 1:02d}",
                "temperature_c": round(55.0 + (idx % 40) * 0.9, 2),
                "vibration_hz": round(10.0 + (idx % 50) * 1.1, 2),
                "pressure_psi": round(90.0 + (idx % 60) * 0.95, 2),
                "rpm": int(1200 + (idx % 2400)),
                "power_draw_kw": round(12.5 + (idx % 35) * 1.0, 2),
                "units_produced": int(5 + (idx % 45)),
                "defect_count": int(idx % 4),
                "status_code": "WARN" if idx % 15 == 0 else "OK",
                "error_code": "NONE",
                "maintenance_flag": False,
                "firmware_version": "v2.4.1"
            }
            f.write(json.dumps(record) + "\n")
print("11/11: Wrote 2,000 unique rows across 5 JSON files in iot_telemetry_stream/")

# ==============================================================================
# PART 3: AUTOMATED ZERO-DUPLICATE AUDIT
# ==============================================================================
audit_checks = [
    ("raw_erp_customers", "customer_id", 1000),
    ("raw_hr_employees", "employee_id", 150),
    ("raw_erp_suppliers", "supplier_id", 50),
    ("raw_wms_warehouses", "warehouse_id", 10),
    ("raw_retail_stores", "store_id", 30),
    ("raw_wms_inventory", "inventory_txn_id", 1500)
]

print("\n--- ZERO-DUPLICATE AUDIT SUMMARY ---")
for tbl, pk, expected in audit_checks:
    row = spark.sql(f"SELECT COUNT(*) AS total, COUNT(DISTINCT {pk}) AS unique_keys FROM {CATALOG}.bronze.{tbl}").collect()[0]
    dups = row["total"] - row["unique_keys"]
    assert dups == 0 and row["total"] == expected, f"Audit failed on {tbl}: total={row['total']}, unique={row['unique_keys']}"
    print(f"VERIFIED {tbl}: {row['total']} total rows | {row['unique_keys']} unique {pk} | {dups} duplicates")

print("\nDay 1 Baseline Data Generation & Zero-Duplicate Verification Complete!")