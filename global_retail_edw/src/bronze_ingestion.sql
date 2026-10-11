-- ==============================================================================
-- ODS LAYER (BRONZE): RAW DATA INGESTION
-- ==============================================================================

-- 1. Ingest Customer Master (CSV)
CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT "Raw Customer data for SCD Type 2 dimension"
AS SELECT 
  *,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/source_files/customer_master',
  format => 'csv',
  header => 'true'
);

-- 2. Ingest Product Catalog (JSON)
CREATE OR REFRESH STREAMING TABLE bronze_products
COMMENT "Raw Product Catalog for standard dimension"
AS SELECT 
  *,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/source_files/product_catalog',
  format => 'json'
);

-- 3. Ingest Sales Transactions (CSV)
CREATE OR REFRESH STREAMING TABLE bronze_sales
COMMENT "Raw Point of Sale transactions"
AS SELECT 
  *,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/source_files/sales_transactions',
  format => 'csv',
  header => 'true'
);