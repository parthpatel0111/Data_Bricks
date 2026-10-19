import dlt
from pyspark.sql.functions import col, expr, concat_ws, when

# Dynamically retrieve the target Unity Catalog from pipeline configuration
CATALOG = spark.conf.get("catalog_name", "retail_mfg_dev")

# ==============================================================================
# PART 1: CDF SOURCE VIEWS (READING CHANGE DATA FEED FROM BRONZE TABLES)
# ==============================================================================

@dlt.view(name="vw_cdf_customers")
def vw_cdf_customers():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.bronze.raw_erp_customers")
        .where("_change_type != 'update_preimage'")
    )

@dlt.view(name="vw_cdf_employees")
def vw_cdf_employees():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.bronze.raw_hr_employees")
        .where("_change_type != 'update_preimage'")
    )

@dlt.view(name="vw_cdf_suppliers")
def vw_cdf_suppliers():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.bronze.raw_erp_suppliers")
        .where("_change_type != 'update_preimage'")
    )

@dlt.view(name="vw_cdf_warehouses")
def vw_cdf_warehouses():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.bronze.raw_wms_warehouses")
        .where("_change_type != 'update_preimage'")
    )

@dlt.view(name="vw_cdf_stores")
def vw_cdf_stores():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.bronze.raw_retail_stores")
        .where("_change_type != 'update_preimage'")
    )

@dlt.view(name="vw_cdf_inventory")
def vw_cdf_inventory():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.bronze.raw_wms_inventory")
        .where("_change_type != 'update_preimage'")
    )

# ==============================================================================
# PART 2: SILVER DIMENSIONS - DECLARATIVE CDC (SCD TYPE 2 & SCD TYPE 1)
# ==============================================================================

# 1. Silver Customers (SCD Type 2 - Tracks history on loyalty tier and location)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_customers",
    comment="SCD Type 2 Customer Master tracking historical changes"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_customers",
    source="vw_cdf_customers",
    keys=["customer_id"],
    sequence_by=col("_updated_at"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=2
)

# 2. Silver Employees (SCD Type 2 - Tracks role and region history)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_employees",
    comment="SCD Type 2 Employee Master tracking role and region changes"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_employees",
    source="vw_cdf_employees",
    keys=["employee_id"],
    sequence_by=col("_updated_at"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=2
)

# 3. Silver Suppliers (SCD Type 1 - Overwrites with latest vendor terms)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_suppliers",
    comment="SCD Type 1 Supplier Master retaining current vendor state"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_suppliers",
    source="vw_cdf_suppliers",
    keys=["supplier_id"],
    sequence_by=col("_updated_at"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1
)

# 4. Silver Warehouses (SCD Type 1 - Overwrites facility attributes)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_warehouses",
    comment="SCD Type 1 Warehouse and Factory Master"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_warehouses",
    source="vw_cdf_warehouses",
    keys=["warehouse_id"],
    sequence_by=col("_updated_at"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1
)

# 5. Silver Retail Stores (SCD Type 2 - Tracks store format & status history)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_stores",
    comment="SCD Type 2 Store Master tracking store reformat and status history"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_stores",
    source="vw_cdf_stores",
    keys=["store_id"],
    sequence_by=col("_updated_at"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=2
)

# 6. Silver Products (SCD Type 2 from Auto Loader Parquet stream)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_products",
    comment="SCD Type 2 Product Master tracking unit cost and price changes"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_products",
    source=f"{CATALOG}.bronze.bronze_products",
    keys=["sku"],
    sequence_by=col("_updated_at"),
    except_column_list=["_source_file_name", "_ingested_at", "_rescued_data"],
    stored_as_scd_type=2
)

# 7. Silver Promotions (SCD Type 1 from Auto Loader CSV stream)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_promotions",
    comment="SCD Type 1 Marketing Promotions Master"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_promotions",
    source=f"{CATALOG}.bronze.bronze_promotions",
    keys=["promo_code"],
    sequence_by=col("_updated_at"),
    except_column_list=["_source_file_name", "_ingested_at", "_rescued_data"],
    stored_as_scd_type=1
)

# ==============================================================================
# PART 3: SILVER FACTS - CLEANSING, QUARANTINE PATTERN & SCHEMA EVOLUTION
# ==============================================================================

# 8. Silver Inventory Transactions (SCD Type 1 via CDF)
dlt.create_streaming_table(
    name=f"{CATALOG}.silver.silver_inventory_transactions",
    comment="Cleansed WMS inventory movements sequenced via CDF"
)
dlt.apply_changes(
    target=f"{CATALOG}.silver.silver_inventory_transactions",
    source="vw_cdf_inventory",
    keys=["inventory_txn_id"],
    sequence_by=col("_updated_at"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    stored_as_scd_type=1
)

# 9. Silver Online Sales (Layered Expectations: FAIL, DROP, WARN)
@dlt.table(
    name=f"{CATALOG}.silver.silver_online_sales",
    comment="Validated e-commerce online sales with type enforcement"
)
@dlt.expect_or_fail("pk_order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_online_quantity", "quantity > 0")
@dlt.expect("valid_online_unit_price", "unit_price >= 0")
def silver_online_sales():
    return (
        dlt.read_stream(f"{CATALOG}.bronze.bronze_online_sales")
        .withColumn("order_ts", col("order_ts").cast("timestamp"))
    )

# 10A. Silver POS Sales - Quarantine Routing Table (Zero Data Loss Pattern)
pos_rules = {
    "valid_receipt": "receipt_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "valid_product": "sku IS NOT NULL",
    "valid_quantity": "quantity > 0",
    "valid_unit_price": "unit_price > 0"
}

@dlt.table(
    name=f"{CATALOG}.silver.silver_pos_sales",
    comment="POS transactions evaluated with WARN expectations and is_quarantined flag",
    partition_cols=["is_quarantined"]
)
@dlt.expect_all(pos_rules)
def silver_pos_sales():
    return (
        dlt.read_stream(f"{CATALOG}.bronze.bronze_pos_sales")
        .withColumn("sale_ts", col("sale_ts").cast("timestamp"))
        .withColumn(
            "is_quarantined",
            expr("NOT (receipt_id IS NOT NULL AND store_id IS NOT NULL AND sku IS NOT NULL AND quantity > 0 AND unit_price > 0)")
        )
        .withColumn(
            "quarantine_reason",
            concat_ws(
                ", ",
                when(col("receipt_id").isNull(), expr("'Missing Receipt ID'")),
                when(col("store_id").isNull(), expr("'Missing Store ID'")),
                when(col("sku").isNull(), expr("'Missing SKU'")),
                when(col("quantity") <= 0, expr("'Zero or Negative Quantity'")),
                when(col("unit_price") <= 0, expr("'Zero or Negative Unit Price'"))
            )
        )
    )

# 10B. Silver POS Sales - Clean Stream (Routed downstream to Gold)
@dlt.table(
    name=f"{CATALOG}.silver.silver_pos_sales_clean",
    comment="Clean, verified POS sales for Gold Star Schema consumption"
)
def silver_pos_sales_clean():
    return (
        dlt.read_stream(f"{CATALOG}.silver.silver_pos_sales")
        .filter("is_quarantined = false")
    )

# 10C. Silver POS Sales - Quarantine Table (Isolated for Data Stewards)
@dlt.table(
    name=f"{CATALOG}.silver.silver_pos_sales_quarantine",
    comment="Isolated defective POS records preserved for root-cause remediation"
)
def silver_pos_sales_quarantine():
    return (
        dlt.read_stream(f"{CATALOG}.silver.silver_pos_sales")
        .filter("is_quarantined = true")
    )

# 11. Silver Manufacturing IoT Telemetry (Schema Evolution Pass-Through)
@dlt.table(
    name=f"{CATALOG}.silver.silver_iot_telemetry",
    comment="Cleansed IoT factory sensor telemetry supporting dynamic schema evolution"
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
def silver_iot_telemetry():
    return (
        dlt.read_stream(f"{CATALOG}.bronze.bronze_iot_telemetry")
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
    )