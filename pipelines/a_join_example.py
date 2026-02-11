from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("spark-join-example").getOrCreate()

    customers = spark.createDataFrame(
        [(1, "alice"), (2, "bob"), (3, "carol")],
        ["customer_id", "name"],
    )
    orders = spark.createDataFrame(
        [(101, 1, 250.0), (102, 1, 80.0), (103, 2, 120.0)],
        ["order_id", "customer_id", "amount"],
    )

    joined = customers.join(orders, on="customer_id", how="left")
    joined.orderBy("customer_id", "order_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
