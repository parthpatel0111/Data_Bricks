import dlt
from pyspark.sql.functions import col, expr

# ==============================================================================
# 1. DIMENSION: PRODUCT (Standard Type 1 Dimension)
# ==============================================================================
@dlt.table(
    name="dim_product",
    comment="Standard Product Dimension"
)
def dim_product():
    # Reads directly from the cleansed Silver layer
    return dlt.read("silver_products")

# ==============================================================================
# 2. DIMENSION: CUSTOMER (SCD Type 2)
# ==============================================================================
dlt.create_streaming_table(
    name="dim_customer",
    comment="SCD Type 2 Customer Dimension tracking historical address changes."
)

dlt.apply_changes(
    target="dim_customer",
    source="bronze_customers", 
    keys=["CustomerID"],
    sequence_by="UpdateTimestamp", 
    track_history_column_list=["City", "State"], 
    except_column_list=["source_file", "ingestion_timestamp"],
    stored_as_scd_type=2  # <-- THIS IS THE REQUIRED FIX
)

# ==============================================================================
# 3. DIMENSION: DATE (Generated via Spark SQL)
# ==============================================================================
@dlt.table(
    name="dim_date",
    comment="Enterprise Date Dimension"
)
def dim_date():
    # Dynamically generates a calendar dimension from 2024 to 2030
    return spark.sql("""
        SELECT 
            CAST(date_col AS DATE) AS DateKey,
            YEAR(date_col) AS Year,
            MONTH(date_col) AS Month,
            DAY(date_col) AS Day,
            DATE_FORMAT(date_col, 'EEEE') AS DayOfWeek,
            QUARTER(date_col) AS Quarter
        FROM (
            SELECT explode(sequence(to_date('2024-01-01'), to_date('2030-12-31'), interval 1 day)) AS date_col
        )
    """)

# ==============================================================================
# 4. FACT: SALES TRANSACTIONS (Optimized with Liquid Clustering)
# ==============================================================================
@dlt.table(
    name="fct_sales",
    comment="Massive Transaction Fact Table optimized for BI aggregations.",
    cluster_by=["CustomerID", "ProductID"] # Optimize for dimension joins
)
def fct_sales():
    return (
        dlt.read("silver_sales")
        .select(
            "OrderID",
            "CustomerID",
            "ProductID",
            "TransactionDate",
            col("TransactionDate").cast("date").alias("DateKey"),
            "Quantity",
            "DiscountAmount",
            # Pre-calculate common metrics for the BI team
            expr("Quantity * 1").alias("ItemCount") 
        )
    )