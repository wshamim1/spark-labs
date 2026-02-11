from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, isnan, isnull


def main() -> None:
    spark = SparkSession.builder.appName("spark-data-quality").getOrCreate()

    data = spark.createDataFrame(
        [
            (1, "alice", 50000),
            (2, None, 75000),
            (3, "carol", None),
            (4, "dave", 80000),
            (5, "eve", -5000),
            (None, "frank", 90000),
        ],
        ["id", "name", "salary"],
    )

    print("=== Data Quality Report ===")
    print(f"Total records: {data.count()}")

    null_report = data.select(
        [
            count(when(col(c).isNull(), c)).alias(f"{c}_nulls")
            for c in data.columns
        ]
    )
    print("\n=== Null Values ===")
    null_report.show(truncate=False)

    negative_salary = data.filter(col("salary") < 0)
    print("\n=== Records with negative salary ===")
    negative_salary.show(truncate=False)

    duplicates = (
        data.groupBy("id", "name", "salary").count().filter(col("count") > 1)
    )
    print("\n=== Duplicate Records ===")
    duplicates.show(truncate=False)

    quality_issues = data.filter(
        col("id").isNull()
        | col("name").isNull()
        | col("salary").isNull()
        | (col("salary") < 0)
    )
    print(f"\n=== Records with quality issues: {quality_issues.count()} ===")
    quality_issues.show(truncate=False)

    clean = data.filter(
        col("id").isNotNull()
        & col("name").isNotNull()
        & col("salary").isNotNull()
        & (col("salary") > 0)
    )
    print(f"\n=== Clean records: {clean.count()} ===")
    clean.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
