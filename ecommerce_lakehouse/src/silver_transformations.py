import dlt
from pyspark.sql.functions import col

# ==============================================================================
# 1. SILVER CLICKSTREAM: Data Quality & The Quarantine Pattern
# ==============================================================================
# In ODI, bad data fails the job or goes to an E$ table. 
# In Databricks, we use @dlt.expect to track bad records without failing the pipeline.

@dlt.table(
    name="silver_clickstream_logs",
    comment="Cleansed clickstream data tracking valid session durations."
)
# This constraint flags rows where duration < 0 but allows the pipeline to continue
@dlt.expect("valid_session_duration", "session_duration >= 0")
def silver_clickstream_logs():
    return (
        dlt.read_stream("bronze_clickstream_logs")
        # Standardize column types
        .withColumn("session_duration", col("session_duration").cast("int"))
        .withColumn("customer_id", col("customer_id").cast("int"))
    )

# ==============================================================================
# 2. SILVER ERP ORDERS: Type Casting & Cleansing (OPTIMIZED)
# ==============================================================================
@dlt.table(
    name="silver_erp_orders",
    comment="Cleansed ERP orders with enforced schema types, optimized for heavy joins using Liquid Clustering.",
    cluster_by=["CustomerID"]  # <-- ADDED LIQUID CLUSTERING HERE
)
@dlt.expect_or_drop("valid_quantity", "Quantity > 0") # Drops rows where Qty is 0 or negative
def silver_erp_orders():
    return (
        dlt.read_stream("bronze_erp_orders")
        .withColumn("Quantity", col("Quantity").cast("int"))
        .withColumn("Amount", col("Amount").cast("double"))
        .withColumn("OrderDate", col("OrderDate").cast("date"))
    )

# ==============================================================================
# 3. SILVER CRM: Change Data Capture (CDC)
# ==============================================================================

dlt.create_streaming_table(
    name="silver_crm_customers",
    comment="Current state of customers utilizing AUTO CDC"
)

dlt.apply_changes(
    target="silver_crm_customers",
    source="bronze_crm_customers",
    keys=["CustomerID"], # Corrected to match the CSV header
    sequence_by="ProcessDate", # Corrected to sequence by the Oracle transaction date
    apply_as_deletes="Operation = 'DELETE'", # Process hard deletes if the Operation column flags it
    except_column_list=["source_file", "ingestion_timestamp"] 
)