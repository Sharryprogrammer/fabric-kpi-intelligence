from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH = "Files/landing/sales_orders.csv"
SILVER_TABLE = "silver_sales"
DAILY_KPI_TABLE = "gold_daily_kpis"
REGION_KPI_TABLE = "gold_region_kpis"
REJECTED_TABLE = "dq_rejected_sales"

schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("region", StringType(), True),
        StructField("product", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(18, 2), False),
        StructField("unit_cost", DecimalType(18, 2), False),
        StructField("status", StringType(), False),
    ]
)


def add_quality_status(df: DataFrame) -> DataFrame:
    reasons = F.array_compact(
        F.array(
            F.when(F.col("order_id").isNull(), F.lit("missing_order_id")),
            F.when(F.col("order_date").isNull(), F.lit("invalid_order_date")),
            F.when(F.trim(F.col("region")) == "", F.lit("missing_region")),
            F.when(F.col("quantity") <= 0, F.lit("non_positive_quantity")),
            F.when(F.col("unit_price") < 0, F.lit("negative_unit_price")),
            F.when(F.col("unit_cost") < 0, F.lit("negative_unit_cost")),
        )
    )
    return df.withColumn("dq_reasons", reasons).withColumn(
        "is_valid", F.size("dq_reasons") == 0
    )


raw = (
    spark.read.option("header", True)
    .option("dateFormat", "yyyy-MM-dd")
    .schema(schema)
    .csv(SOURCE_PATH)
)

checked = add_quality_status(raw)

rejected = checked.filter(~F.col("is_valid"))
rejected.write.mode("overwrite").format("delta").saveAsTable(REJECTED_TABLE)

silver = (
    checked.filter(F.col("is_valid"))
    .drop("dq_reasons", "is_valid")
    .withColumn("region", F.initcap(F.trim("region")))
    .withColumn("status", F.initcap(F.trim("status")))
    .withColumn(
        "gross_revenue",
        (F.col("quantity") * F.col("unit_price")).cast(DecimalType(20, 2)),
    )
    .withColumn(
        "gross_cost",
        (F.col("quantity") * F.col("unit_cost")).cast(DecimalType(20, 2)),
    )
    .withColumn(
        "gross_margin",
        (F.col("gross_revenue") - F.col("gross_cost")).cast(DecimalType(20, 2)),
    )
    .withColumn("ingested_at_utc", F.current_timestamp())
)

silver.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(SILVER_TABLE)

completed = silver.filter(F.col("status") == "Completed")

daily_kpis = completed.groupBy("order_date").agg(
    F.countDistinct("order_id").alias("completed_orders"),
    F.sum("quantity").alias("units_sold"),
    F.sum("gross_revenue").alias("revenue"),
    F.sum("gross_margin").alias("gross_margin"),
    F.round(F.sum("gross_revenue") / F.countDistinct("order_id"), 2).alias(
        "average_order_value"
    ),
)

region_kpis = completed.groupBy("region").agg(
    F.countDistinct("order_id").alias("completed_orders"),
    F.sum("quantity").alias("units_sold"),
    F.sum("gross_revenue").alias("revenue"),
    F.sum("gross_margin").alias("gross_margin"),
    F.round(
        F.sum("gross_margin") / F.when(F.sum("gross_revenue") == 0, None).otherwise(
            F.sum("gross_revenue")
        ),
        4,
    ).alias("gross_margin_rate"),
)

daily_kpis.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(DAILY_KPI_TABLE)
region_kpis.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(REGION_KPI_TABLE)

print(
    {
        "accepted_rows": silver.count(),
        "rejected_rows": rejected.count(),
        "silver_table": SILVER_TABLE,
        "daily_kpi_table": DAILY_KPI_TABLE,
        "region_kpi_table": REGION_KPI_TABLE,
    }
)
