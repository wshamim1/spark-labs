from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("spark-example-job")
        .getOrCreate()
    )

    data = [("alice", 1), ("bob", 2), ("alice", 3), ("bob", 4), ("carol", 5)]
    df = spark.createDataFrame(data, ["name", "value"])

    result = (
        df.groupBy("name")
        .sum("value")
        .orderBy("name")
    )

    result.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
