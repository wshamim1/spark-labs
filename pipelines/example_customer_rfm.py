from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    min,
    datediff,
    current_date,
)


def main() -> None:
    spark = SparkSession.builder.appName("spark-customer-rfm-analysis").getOrCreate()

    customers = spark.createDataFrame(
        [
            (1, "alice"),
            (2, "bob"),
            (3, "carol"),
            (4, "dave"),
            (5, "eve"),
        ],
        ["customer_id", "name"],
    )

    orders = spark.createDataFrame(
        [
            (101, 1, "2025-12-01", 150.0),
            (102, 1, "2025-11-15", 75.0),
            (103, 2, "2025-10-20", 200.0),
            (104, 3, "2025-09-10", 50.0),
            (105, 3, "2025-08-05", 120.0),
            (106, 3, "2025-07-01", 90.0),
            (107, 4, "2024-05-01", 300.0),
            (108, 5, "2025-12-15", 400.0),
        ],
        ["order_id", "customer_id", "order_date", "amount"],
    )

    rfm = (
        orders.groupBy("customer_id")
        .agg(
            max("order_date").alias("last_order_date"),
            count("order_id").alias("frequency"),
            sum("amount").alias("monetary"),
        )
        .withColumn(
            "recency_days",
            datediff(current_date(), col("last_order_date")),
        )
        .select("customer_id", "recency_days", "frequency", "monetary")
    )

    rfm_with_names = rfm.join(customers, on="customer_id").select(
        "customer_id", "name", "recency_days", "frequency", "monetary"
    )

    print("=== Customer RFM Analysis ===")
    rfm_with_names.orderBy(col("monetary").desc()).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
