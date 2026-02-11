from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, trim


def main() -> None:
    spark = SparkSession.builder.appName("spark-etl-pipeline").getOrCreate()

    raw_data = spark.createDataFrame(
        [
            (1, "alice  ", "alice@example.com", 50000, "2025-01-15"),
            (2, None, "bob@example.com", 75000, "2025-02-20"),
            (3, "carol", "  ", 80000, "2025-01-10"),
            (4, "dave", "dave@example.com", None, "2025-03-05"),
            (5, "eve", "eve@example.com", 95000, "2025-01-25"),
        ],
        ["id", "name", "email", "salary", "hire_date"],
    )

    print("=== Raw Data ===")
    raw_data.show(truncate=False)

    cleaned = (
        raw_data.filter(col("id").isNotNull())
        .withColumn("name", trim(col("name")))
        .withColumn("name", when(col("name") == "", None).otherwise(col("name")))
        .withColumn(
            "email",
            when(
                (col("email").isNull()) | (trim(col("email")) == ""), None
            ).otherwise(trim(col("email"))),
        )
        .withColumn("salary", when(col("salary").isNull(), 0).otherwise(col("salary")))
        .dropna(thresh=3)
    )

    print("\n=== Cleaned Data ===")
    cleaned.show(truncate=False)

    enriched = cleaned.withColumn(
        "salary_band",
        when(col("salary") < 50000, "junior")
        .when(col("salary") < 80000, "mid")
        .otherwise("senior"),
    )

    print("\n=== Enriched Data ===")
    enriched.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
