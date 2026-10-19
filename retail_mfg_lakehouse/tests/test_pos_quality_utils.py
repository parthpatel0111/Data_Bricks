from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from src.pos_quality_utils import evaluate_pos_quarantine

def test_evaluate_pos_quarantine(spark):
    input_schema = StructType([
        StructField("receipt_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True)
    ])

    input_data = [
        ("POS-001", "STR-001", "PROD-001", 2, 50.0, 10.0),   # Valid record -> is_quarantined = False, net = 90.0
        ("POS-002", "STR-001", "PROD-002", 0, 25.0, 0.0),    # Invalid qty=0 -> is_quarantined = True
        ("POS-003", "STR-002", "PROD-003", 1, -15.0, 0.0),   # Invalid price<0 -> is_quarantined = True
        (None, "STR-003", "PROD-004", 3, 20.0, 5.0)          # Missing receipt_id -> is_quarantined = True
    ]
    input_df = spark.createDataFrame(input_data, schema=input_schema)

    expected_schema = StructType([
        StructField("receipt_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("is_quarantined", BooleanType(), True),
        StructField("net_line_amount", DoubleType(), True)
    ])

    expected_data = [
        ("POS-001", "STR-001", "PROD-001", 2, 50.0, 10.0, False, 90.0),
        ("POS-002", "STR-001", "PROD-002", 0, 25.0, 0.0, True, 0.0),
        ("POS-003", "STR-002", "PROD-003", 1, -15.0, 0.0, True, -15.0),
        (None, "STR-003", "PROD-004", 3, 20.0, 5.0, True, 55.0)
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    actual_df = evaluate_pos_quarantine(input_df)

    # 1. Validate Schema Equality
    assertSchemaEqual(actual_df.schema, expected_df.schema)

    # 2. Validate Data & Row Equality
    assertDataFrameEqual(actual_df, expected_df)