-- ============================================================================
-- BRONZE LAYER: AUTO LOADER FILE INGESTION (5 STREAMING TABLES)
-- ============================================================================

-- 1. Online Sales Stream (JSON -> Rescue Mode for unmapped fields)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.bronze.bronze_online_sales
COMMENT "Raw e-commerce online sales ingested incrementally via Auto Loader"
AS SELECT
  *,
  _metadata.file_name AS _source_file_name,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/${catalog_name}/landing_volume/source_files/online_sales_stream',
  format => 'json',
  schemaEvolutionMode => 'rescue',
  schemaHints => 'order_line_number INT, order_date_key INT, quantity INT, unit_price DECIMAL(10,2), discount_amount DECIMAL(10,2), tax_amount DECIMAL(10,2), shipping_amount DECIMAL(10,2), total_amount DECIMAL(10,2)'
);

-- 2. Store POS Sales Stream (JSON -> Raw landing prior to Silver Quarantine)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.bronze.bronze_pos_sales
COMMENT "Raw point-of-sale transactions including unvalidated records"
AS SELECT
  *,
  _metadata.file_name AS _source_file_name,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/${catalog_name}/landing_volume/source_files/pos_sales_stream',
  format => 'json',
  schemaEvolutionMode => 'rescue',
  schemaHints => 'line_number INT, sale_date_key INT, quantity INT, unit_price DECIMAL(10,2), discount_amount DECIMAL(10,2), tax_amount DECIMAL(10,2), total_amount DECIMAL(10,2), loyalty_used_flag BOOLEAN'
);

-- 3. Manufacturing IoT Telemetry Stream (JSON -> Native Schema Evolution)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.bronze.bronze_iot_telemetry
COMMENT "High-velocity IoT factory telemetry with automatic schema evolution"
AS SELECT
  *,
  _metadata.file_name AS _source_file_name,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/${catalog_name}/landing_volume/source_files/iot_telemetry_stream',
  format => 'json',
  schemaEvolutionMode => 'addNewColumns',
  inferColumnTypes => 'true',
  schemaHints => 'event_date_key INT, temperature_c DOUBLE, vibration_hz DOUBLE, pressure_psi DOUBLE, rpm INT, power_draw_kw DOUBLE, units_produced INT, defect_count INT, maintenance_flag BOOLEAN'
);

-- 4. Marketing Promotions Drop (CSV)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.bronze.bronze_promotions
COMMENT "Marketing promotion calendar CSV drops"
AS SELECT
  *,
  _metadata.file_name AS _source_file_name,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/${catalog_name}/landing_volume/source_files/promotions_csv',
  format => 'csv',
  header => 'true',
  schemaHints => 'discount_value DECIMAL(10,2), start_date DATE, end_date DATE, budget_allocated DECIMAL(12,2), is_active BOOLEAN, _updated_at TIMESTAMP'
);

-- 5. Product Catalog Drop (Parquet)
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.bronze.bronze_products
COMMENT "Master product catalog Parquet drops"
AS SELECT
  *,
  _metadata.file_name AS _source_file_name,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/${catalog_name}/landing_volume/source_files/product_catalog_parquet',
  format => 'parquet'
);