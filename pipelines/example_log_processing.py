from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    date_format,
    lit,
    regexp_extract,
    to_timestamp,
    when,
)


def main() -> None:
    spark = SparkSession.builder.appName("spark-log-processing").getOrCreate()

    raw_logs = spark.createDataFrame(
        [
            ("2026-02-10 10:01:05", "INFO", "auth-service", "login success user=alice"),
            ("2026-02-10 10:02:10", "WARN", "auth-service", "token expiring user=bob"),
            (
                "2026-02-10 10:03:22",
                "ERROR",
                "payment-service",
                "charge failed code=E42 amount=19.99",
            ),
            (
                "2026-02-10 10:04:12",
                "ERROR",
                "payment-service",
                "refund failed code=E13 amount=5.00",
            ),
            ("2026-02-10 10:05:44", "INFO", "orders", "order created id=1001"),
            ("2026-02-10 10:06:01", "INFO", "orders", "order shipped id=1001"),
            ("2026-02-10 10:07:17", "WARN", "orders", "delay warning id=1002"),
        ],
        ["ts", "level", "service", "message"],
    )

    logs = (
        raw_logs.withColumn("timestamp", to_timestamp(col("ts")))
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
        .withColumn("error_code", regexp_extract(col("message"), r"code=([A-Z0-9]+)", 1))
        .drop("ts")
    )

    print("=== Raw Logs ===")
    logs.show(truncate=False)

    by_level = logs.groupBy("level").agg(count(lit(1)).alias("count"))
    print("\n=== Log Level Counts ===")
    by_level.show(truncate=False)

    error_rate = (
        logs.groupBy("date")
        .agg(
            count(lit(1)).alias("total"),
            count(when(col("level") == "ERROR", True)).alias("errors"),
        )
        .withColumn("error_rate", col("errors") / col("total"))
    )
    print("\n=== Error Rate by Day ===")
    error_rate.show(truncate=False)

    top_error_codes = (
        logs.filter(col("level") == "ERROR")
        .groupBy("error_code")
        .agg(count(lit(1)).alias("count"))
        .orderBy(col("count").desc())
    )
    print("\n=== Top Error Codes ===")
    top_error_codes.show(truncate=False)

    service_volume = (
        logs.groupBy("service", "level")
        .agg(count(lit(1)).alias("count"))
        .orderBy(col("count").desc())
    )
    print("\n=== Service Volume by Level ===")
    service_volume.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
