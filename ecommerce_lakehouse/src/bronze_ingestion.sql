-- D:\DATABRICKS_TRAINING\ecommerce_lakehouse\src\bronze_ingestion.sql

-- ==============================================================================
-- WEEK 1: BRONZE LAYER (RAW DATA INGESTION)
-- REPLACING ODI FILE POLLING WITH DATABRICKS AUTO LOADER
-- ==============================================================================

-- 1. Ingest CRM Customers (CSV)
CREATE OR REFRESH STREAMING TABLE bronze_crm_customers
COMMENT "Raw CRM Customer Data ingested incrementally via Auto Loader"
AS SELECT 
  *,
  -- We extract file metadata for auditing (Replacing ODI Journalizing Metadata)
  _metadata.file_name AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/source_files/crm_daily',
  format => 'csv',
  header => 'true'
);

-- 2. Ingest Clickstream Logs (JSON)
CREATE OR REFRESH STREAMING TABLE bronze_clickstream_logs
COMMENT "Raw Web Clickstream JSON Data"
AS SELECT 
  *,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/source_files/clickstream_daily',
  format => 'json'
);

-- 3. Ingest ERP Orders (CSV)
CREATE OR REFRESH STREAMING TABLE bronze_erp_orders
COMMENT "Raw ERP Transactional Orders"
AS SELECT 
  *,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/source_files/erp_daily',
  format => 'csv',
  header => 'true'
);