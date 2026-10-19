from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, round as spark_round

def evaluate_pos_quarantine(df: DataFrame) -> DataFrame:
    """
    Modular PySpark transformation that evaluates POS data quality rules,
    flags defective rows with is_quarantined, and calculates net_line_amount.
    """
    return (
        df.withColumn(
            "is_quarantined",
            expr("NOT (receipt_id IS NOT NULL AND store_id IS NOT NULL AND sku IS NOT NULL AND quantity > 0 AND unit_price > 0)")
        )
        .withColumn(
            "net_line_amount",
            spark_round((col("quantity") * col("unit_price")) - col("discount_amount"), 2)
        )
    )