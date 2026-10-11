import dlt
from pyspark.sql.functions import col, sum, count

# ==============================================================================
# GOLD LAYER: Business Aggregations (Materialized Views)
# ==============================================================================
# We use dlt.read() instead of dlt.read_stream() here because we are calculating
# full aggregations across the entire historical dataset for BI reporting.

@dlt.table(
    name="gold_customer_360",
    comment="Materialized View combining CRM profiles with total ERP spend and order counts."
)
def gold_customer_360():
    # Read the cleansed Silver tables
    crm = dlt.read("silver_crm_customers")
    erp = dlt.read("silver_erp_orders")
    
    # Calculate total spend and order count per customer
    customer_spend = (
        erp.groupBy("CustomerID")
        .agg(
            count("OrderID").alias("total_orders"),
            sum("Amount").alias("total_lifetime_spend")
        )
    )
    
    # Join the aggregated spend with the CRM customer profile
    return (
        crm.join(customer_spend, "CustomerID", "left")
        .select(
            crm["CustomerID"],
            crm["Name"],
            crm["Email"],
            crm["Region"],
            customer_spend["total_orders"],
            customer_spend["total_lifetime_spend"]
        )
    )