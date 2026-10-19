import json
from datetime import datetime, date, timedelta
from decimal import Decimal
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, DateType, TimestampType, DecimalType
)

CATALOG = "retail_mfg_dev"
BASE_VOLUME_PATH = f"/Volumes/{CATALOG}/landing_volume/source_files"
DAY2_TS = datetime(2026, 10, 15, 10, 0, 0)

print(f"Starting Day 2 Incremental CDC & Edge-Case Injection for {CATALOG}...")

# ==============================================================================
# 1. SCD TYPE 2 TEST: UPDATE 10 CUSTOMERS + INSERT 5 NEW CUSTOMERS (CDF)
# ==============================================================================
spark.sql(f"""
UPDATE {CATALOG}.bronze.raw_erp_customers
SET loyalty_tier = 'Platinum',
    city = 'Miami',
    country_code = 'US',
    _updated_at = TIMESTAMP('{DAY2_TS.isoformat()}')
WHERE customer_id IN (
    'CUST-0001', 'CUST-0002', 'CUST-0003', 'CUST-0004', 'CUST-0005',
    'CUST-0006', 'CUST-0007', 'CUST-0008', 'CUST-0009', 'CUST-0010'
)
""")

new_customers_schema = StructType([
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

new_customers_data = [
    (f"CUST-{i:04d}", f"NewUser{i}", "Patel", f"newuser.{i}@example.com",
     f"+1-555-900-{i:04d}", date(1995, 5, 15), "Gold", "Austin", "US", DAY2_TS)
    for i in range(1001, 1006)
]
spark.createDataFrame(new_customers_data, schema=new_customers_schema).write.format("delta").mode("append").saveAsTable(f"{CATALOG}.bronze.raw_erp_customers")
print("1/6: Updated 10 existing customers & inserted 5 new customers in bronze.raw_erp_customers (SCD2 test)")

# ==============================================================================
# 2. SCD TYPE 1 TEST: UPDATE 5 SUPPLIERS IN PLACE (CDF)
# ==============================================================================
spark.sql(f"""
UPDATE {CATALOG}.bronze.raw_erp_suppliers
SET payment_terms = 'NET90',
    contract_tier = 'Strategic',
    _updated_at = TIMESTAMP('{DAY2_TS.isoformat()}')
WHERE supplier_id IN ('SUP-001', 'SUP-002', 'SUP-003', 'SUP-004', 'SUP-005')
""")
print("2/6: Updated 5 suppliers in bronze.raw_erp_suppliers (SCD1 overwrite test)")

# ==============================================================================
# 3. WMS INVENTORY CDF TEST: INSERT 100 NEW TRANSACTIONS
# ==============================================================================
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

day2_inventory = [
    (
        f"ITXN-{i:05d}", f"WH-{((i - 1) % 10) + 1:03d}", f"STR-{((i - 1) % 30) + 1:03d}",
        f"PROD-{((i - 1) % 200) + 1:03d}", f"SUP-{((i - 1) % 50) + 1:03d}", "Receive",
        int(50), int(1200), "EA", "AISLE-01-BIN-01", f"LOT-2026-{i:05d}",
        date(2027, 12, 28), "EMP-001", f"PO-2026-{i:05d}", Decimal("45.00"),
        False, "WMS_CORE_V2", DAY2_TS
    )
    for i in range(1501, 1601)
]
spark.createDataFrame(day2_inventory, schema=inventory_schema).write.format("delta").mode("append").saveAsTable(f"{CATALOG}.bronze.raw_wms_inventory")
print("3/6: Appended 100 new inventory transactions to bronze.raw_wms_inventory")

# ==============================================================================
# 4. ONLINE SALES STREAM: DROP DAY 2 FILE WITH NEW UNMAPPED FIELD (_rescued_data)
# ==============================================================================
online_day2_path = f"{BASE_VOLUME_PATH}/online_sales_stream/online_orders_day2_part06.json"
with open(online_day2_path, "w", encoding="utf-8") as f:
    for idx in range(1501, 1601):
        event_dt = DAY2_TS - timedelta(minutes=(idx - 1500))
        record = {
            "order_id": f"ORD-2026-{idx:05d}",
            "order_line_number": 1,
            "order_ts": event_dt.isoformat(),
            "order_date_key": int(event_dt.strftime("%Y%m%d")),
            "customer_id": f"CUST-{((idx - 1) % 1000) + 1:04d}",
            "sku": f"PROD-{((idx - 1) % 200) + 1:03d}",
            "promo_code": f"PROMO-{((idx - 1) % 25) + 1:02d}",
            "warehouse_id": f"WH-{((idx - 1) % 5) + 1:03d}",
            "sales_channel": "Mobile App",
            "quantity": 2,
            "unit_price": 50.00,
            "discount_amount": 0.00,
            "tax_amount": 8.00,
            "shipping_amount": 5.00,
            "total_amount": 113.00,
            "currency_code": "USD",
            "payment_method": "Credit Card",
            "shipping_status": "Shipped",
            "device_type": "iOS",
            "session_id": f"SESS-2026-{idx:06d}"
        }
        # Inject brand-new post-baseline field on every 10th row (10 rows total) to trigger _rescued_data
        if idx % 10 == 0:
            record["day2_flash_coupon_meta"] = "VIP_WINTER_2026"
        f.write(json.dumps(record) + "\n")
print("4/6: Dropped online_orders_day2_part06.json (100 rows, 10 with new unmapped field for _rescued_data)")

# ==============================================================================
# 5. POS SALES STREAM: DROP DAY 2 FILE (90 CLEAN + 10 QUARANTINE DEFECTIVE ROWS)
# ==============================================================================
pos_day2_path = f"{BASE_VOLUME_PATH}/pos_sales_stream/pos_txns_day2_part06.json"
with open(pos_day2_path, "w", encoding="utf-8") as f:
    for j in range(1, 101):
        idx = 1500 + j
        is_bad = (j <= 10)
        qty = 0 if (is_bad and j <= 5) else 2
        u_price = -19.99 if (is_bad and j > 5) else 40.00
        tot = round(qty * u_price, 2)
        event_dt = DAY2_TS - timedelta(minutes=j)
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
            "discount_amount": 0.0,
            "tax_amount": 0.0,
            "total_amount": tot,
            "register_id": "REG-01",
            "transaction_type": "Sale",
            "loyalty_used_flag": True,
            "payment_type": "Card",
            "shift_id": "MORNING"
        }
        f.write(json.dumps(record) + "\n")
print("5/6: Dropped pos_txns_day2_part06.json (90 clean + 10 defective quarantine rows)")

# ==============================================================================
# 6. IOT TELEMETRY STREAM: DROP DAY 2 FILE WITH NEW SENSOR COLUMN (SCHEMA EVOLUTION)
# ==============================================================================
iot_day2_path = f"{BASE_VOLUME_PATH}/iot_telemetry_stream/iot_sensors_day2_part06.json"
with open(iot_day2_path, "w", encoding="utf-8") as f:
    for j in range(1, 201):
        idx = 2000 + j
        event_dt = DAY2_TS - timedelta(minutes=j)
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
            "temperature_c": 72.5,
            "vibration_hz": 24.0,
            "pressure_psi": 115.0,
            "rpm": 2400,
            "power_draw_kw": 30.5,
            "units_produced": 25,
            "defect_count": 0,
            "status_code": "OK",
            "error_code": "NONE",
            "maintenance_flag": False,
            "firmware_version": "v2.5.0",
            # NEW SENSOR COLUMN INTRODUCED ON DAY 2 FOR AUTO LOADER SCHEMA EVOLUTION:
            "ambient_humidity_pct": round(40.0 + (j % 25) * 0.75, 2)
        }
        f.write(json.dumps(record) + "\n")
print("6/6: Dropped iot_sensors_day2_part06.json (200 rows with new 'ambient_humidity_pct' column)")

print("\nDay 2 Incremental Data Injection Complete!")