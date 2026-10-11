import dlt
from pyspark.sql.functions import col

# 1. Staged Product Catalog (Cleansing)
@dlt.table(
    name="silver_products",
    comment="Cleansed product catalog with enforced types."
)
def silver_products():
    return (
        dlt.read_stream("bronze_products")
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))
    )

# 2. Staged Sales Transactions (Cleansing)
@dlt.table(
    name="silver_sales",
    comment="Cleansed sales transactions."
)
def silver_sales():
    return (
        dlt.read_stream("bronze_sales")
        .withColumn("Quantity", col("Quantity").cast("int"))
        .withColumn("DiscountAmount", col("DiscountAmount").cast("double"))
        .withColumn("TransactionDate", col("TransactionDate").cast("timestamp"))
    )

# 3. Staged Customers (Change Data Capture)
dlt.create_streaming_table(
    name="silver_customers",
    comment="ODS Customer table with CDC applied to maintain current state."
)

dlt.apply_changes(
    target="silver_customers",
    source="bronze_customers",
    keys=["CustomerID"],
    sequence_by="UpdateTimestamp", 
    except_column_list=["source_file", "ingestion_timestamp"]
)