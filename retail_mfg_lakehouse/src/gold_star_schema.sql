-- ============================================================================
-- GOLD LAYER: 8 DIMENSION TABLES (STAR SCHEMA REFERENCE LAYER)
-- ============================================================================

-- 1. dim_date (Static Generated Calendar Materialized View - 365 rows for 2026)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_date
COMMENT "Gold Date Dimension generated for calendar year 2026"
AS SELECT
  CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
  calendar_date,
  DAYOFWEEK(calendar_date) AS day_of_week,
  date_format(calendar_date, 'EEEE') AS day_name,
   (DAYOFWEEK(calendar_date) IN (1, 7)) AS is_weekend,
  WEEKOFYEAR(calendar_date) AS week_of_year,
  MONTH(calendar_date) AS calendar_month,
  date_format(calendar_date, 'MMMM') AS month_name,
  QUARTER(calendar_date) AS calendar_quarter,
  YEAR(calendar_date) AS calendar_year,
  CASE WHEN date_format(calendar_date, 'MM-dd') IN ('01-01', '07-04', '12-25') THEN true ELSE false END AS is_holiday,
  CASE
    WHEN date_format(calendar_date, 'MM-dd') = '01-01' THEN 'New Year Day'
    WHEN date_format(calendar_date, 'MM-dd') = '07-04' THEN 'Independence Day'
    WHEN date_format(calendar_date, 'MM-dd') = '12-25' THEN 'Christmas Day'
    ELSE 'None'
  END AS holiday_name
FROM (
  SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-12-31'), interval 1 day)) AS calendar_date
);

-- 2. dim_customer (SCD Type 2 Dimension)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_customer
COMMENT "Gold Customer Dimension with SCD Type 2 history"
AS SELECT
  xxhash64(customer_id) AS customer_key,
  customer_id,
  first_name,
  last_name,
  email_address,
  phone_number,
  date_of_birth,
  loyalty_tier,
  city,
  country_code,
  __START_AT AS _START_AT,
  __END_AT AS _END_AT
FROM ${catalog_name}.silver.silver_customers;

-- 3. dim_employee (SCD Type 2 Dimension)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_employee
COMMENT "Gold Employee Dimension with SCD Type 2 history and regional RLS attribute"
AS SELECT
  xxhash64(employee_id) AS employee_key,
  employee_id,
  full_name,
  job_title,
  department,
  region_code,
  manager_employee_id,
  hire_date,
  is_active,
  employment_status,
  __START_AT AS _START_AT,
  __END_AT AS _END_AT
FROM ${catalog_name}.silver.silver_employees;

-- 4. dim_supplier (SCD Type 1 Dimension)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_supplier
COMMENT "Gold Supplier Dimension (SCD Type 1 current state)"
AS SELECT
  xxhash64(supplier_id) AS supplier_key,
  supplier_id,
  supplier_name,
  contact_name,
  supplier_category,
  contract_tier,
  payment_terms,
  lead_time_days,
  country,
  risk_rating,
  last_audit_date,
  is_active,
  _updated_at
FROM ${catalog_name}.silver.silver_suppliers;

-- 5. dim_product (SCD Type 2 Dimension)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_product
COMMENT "Gold Product Dimension with SCD Type 2 cost and price history"
AS SELECT
  xxhash64(sku) AS product_key,
  sku,
  product_name,
  brand,
  category,
  sub_category,
  product_type,
  unit_cost,
  unit_price,
  weight_kg,
  lifecycle_status,
  __START_AT AS _START_AT,
  __END_AT AS _END_AT
FROM ${catalog_name}.silver.silver_products;

-- 6. dim_store (SCD Type 2 Dimension)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_store
COMMENT "Gold Store Dimension with SCD Type 2 format and status history"
AS SELECT
  xxhash64(store_id) AS store_key,
  store_id,
  store_name,
  store_type,
  square_footage,
  city,
  region_code,
  country_code,
  manager_name,
  open_date,
  store_status,
  __START_AT AS _START_AT,
  __END_AT AS _END_AT
FROM ${catalog_name}.silver.silver_stores;

-- 7. dim_warehouse (SCD Type 1 Dimension)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_warehouse
COMMENT "Gold Warehouse and Factory Facility Dimension (SCD Type 1)"
AS SELECT
  xxhash64(warehouse_id) AS warehouse_key,
  warehouse_id,
  facility_name,
  facility_type,
  address,
  city,
  state_province,
  country,
  region_code,
  capacity_pallets,
  manager_employee_id,
  is_active,
  _updated_at
FROM ${catalog_name}.silver.silver_warehouses;

-- 8. dim_promotion (SCD Type 1 Dimension)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.dim_promotion
COMMENT "Gold Marketing Promotion Dimension (SCD Type 1)"
AS SELECT
  xxhash64(promo_code) AS promotion_key,
  promo_code,
  campaign_name,
  discount_type,
  discount_value,
  start_date,
  end_date,
  channel,
  budget_allocated,
  is_active,
  _updated_at
FROM ${catalog_name}.silver.silver_promotions;

-- ============================================================================
-- GOLD LAYER: 4 FACT TABLES (LIQUID CLUSTERING ENABLED)
-- ============================================================================

-- 9. fct_online_sales (Streaming Table with Liquid Clustering & GDPR DML Support)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.gold.fct_online_sales
CLUSTER BY (order_date_key, customer_key)
COMMENT "Gold E-Commerce Online Sales Fact Table"
AS SELECT
  xxhash64(s.order_id, s.order_line_number) AS online_sale_key,
  s.order_id,
  s.order_line_number,
  s.order_ts,
  s.order_date_key,
  xxhash64(s.customer_id) AS customer_key,
  s.customer_id,
  xxhash64(s.sku) AS product_key,
  s.sku,
  xxhash64(s.promo_code) AS promotion_key,
  xxhash64(s.warehouse_id) AS fulfillment_warehouse_key,
  s.sales_channel,
  s.quantity,
  s.unit_price,
  s.discount_amount,
  s.tax_amount,
  s.shipping_amount,
  s.total_amount,
  s.currency_code,
  s.payment_method,
  s.shipping_status,
  s.device_type,
  s.session_id,
  s._source_file_name,
  s._ingested_at,
  s._rescued_data
FROM STREAM(${catalog_name}.silver.silver_online_sales) s;

-- 10. fct_store_sales (Streaming Table fed strictly from Clean Non-Quarantined POS Stream)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.gold.fct_store_sales
CLUSTER BY (sale_date_key, store_key)
COMMENT "Gold Point-of-Sale Fact Table containing verified non-quarantined sales"
AS SELECT
  xxhash64(s.receipt_id, s.line_number) AS store_sale_key,
  s.receipt_id,
  s.line_number,
  s.sale_ts,
  s.sale_date_key,
  xxhash64(s.store_id) AS store_key,
  s.store_id,
  xxhash64(s.employee_id) AS employee_key,
  xxhash64(s.customer_id) AS customer_key,
  xxhash64(s.sku) AS product_key,
  xxhash64(s.promo_code) AS promotion_key,
  s.quantity,
  s.unit_price,
  s.discount_amount,
  s.tax_amount,
  s.total_amount,
  s.register_id,
  s.transaction_type,
  s.loyalty_used_flag,
  s.payment_type,
  s.shift_id,
  s.is_quarantined,
  s.quarantine_reason,
  s._source_file_name,
  s._ingested_at,
  s._rescued_data
FROM STREAM(${catalog_name}.silver.silver_pos_sales_clean) s;

-- 11. fct_inventory_transactions (Materialized View over Silver CDC Inventory Table)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.fct_inventory_transactions
CLUSTER BY (txn_date_key, warehouse_key, product_key)
COMMENT "Gold Warehouse Inventory Transactions Fact Table"
AS SELECT
  xxhash64(s.inventory_txn_id) AS inventory_txn_key,
  s.inventory_txn_id,
  CAST(date_format(s._updated_at, 'yyyyMMdd') AS INT) AS txn_date_key,
  xxhash64(s.warehouse_id) AS warehouse_key,
  xxhash64(s.destination_store_id) AS destination_store_key,
  xxhash64(s.product_id) AS product_key,
  xxhash64(s.supplier_id) AS supplier_key,
  s.txn_type,
  s.quantity_changed,
  s.quantity_on_hand,
  s.unit_of_measure,
  s.bin_location,
  s.lot_number,
  s.expiration_date,
  s.operator_id,
  s.reference_doc_id,
  s.cost_basis,
  CAST(s.quantity_changed * s.cost_basis AS DECIMAL(12,2)) AS total_cost,
  s.is_reconciliation,
  s.source_system,
  s._change_type,
  s._commit_version AS cdf_commit_version,
  s._commit_timestamp AS cdf_commit_timestamp,
  s._updated_at
FROM ${catalog_name}.silver.silver_inventory_transactions s;

-- 12. fct_manufacturing_telemetry (Streaming Table with Schema Evolution Pass-Through)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.gold.fct_manufacturing_telemetry
CLUSTER BY (event_date_key, facility_key, device_id)
COMMENT "Gold Manufacturing IoT Telemetry Fact Table with Native Schema Evolution"
AS SELECT
  xxhash64(s.event_id) AS telemetry_key,
  xxhash64(s.facility_id) AS facility_key,
  xxhash64(s.sku) AS product_key,
  xxhash64(s.operator_employee_id) AS operator_employee_key,
  s.*
FROM STREAM(${catalog_name}.silver.silver_iot_telemetry) s;

-- ============================================================================
-- CROSS-TABLE RECONCILIATION VIEW (INTEGRATION TEST GATE)
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.val_inventory_reconciliation (
  CONSTRAINT inventory_counts_reconciled EXPECT (bronze_count = gold_count)
)
COMMENT "Cross-table validation comparing Bronze and Gold inventory transaction counts"
AS SELECT
  (SELECT COUNT(*) FROM ${catalog_name}.bronze.raw_wms_inventory) AS bronze_count,
  (SELECT COUNT(*) FROM ${catalog_name}.gold.fct_inventory_transactions) AS gold_count;