from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    when,
    round,
    desc,
)


def main() -> None:
    spark = SparkSession.builder.appName("spark-sales-analysis").getOrCreate()

    sales = spark.createDataFrame(
        [
            (101, 1, "electronics", 1200),
            (102, 1, "electronics", 450),
            (103, 2, "clothing", 150),
            (104, 2, "home", 800),
            (105, 3, "electronics", 2500),
            (106, 3, "clothing", 200),
            (107, 4, "home", 600),
            (108, 5, "electronics", 3000),
        ],
        ["order_id", "store_id", "category", "amount"],
    )

    stores = spark.createDataFrame(
        [
            (1, "Downtown"),
            (2, "Mall"),
            (3, "Airport"),
            (4, "Harbor"),
            (5, "Plaza"),
        ],
        ["store_id", "store_name"],
    )

    print("=== Sales by Category ===")
    category_sales = (
        sales.groupBy("category")
        .agg(
            count("order_id").alias("order_count"),
            sum("amount").alias("total_sales"),
            round(avg("amount"), 2).alias("avg_order"),
        )
        .orderBy(desc("total_sales"))
    )
    category_sales.show(truncate=False)

    print("\n=== Store Performance ===")
    store_sales = (
        sales.join(stores, on="store_id", how="inner")
        .groupBy("store_id", "store_name")
        .agg(
            count("order_id").alias("orders"),
            sum("amount").alias("total_sales"),
        )
        .orderBy(desc("total_sales"))
    )
    store_sales.show(truncate=False)

    print("\n=== Category Performance by Store ===")
    detailed = (
        sales.join(stores, on="store_id")
        .groupBy("store_name", "category")
        .agg(
            count("order_id").alias("orders"),
            sum("amount").alias("sales"),
        )
        .orderBy(desc("sales"))
    )
    detailed.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
