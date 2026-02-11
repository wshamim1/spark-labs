from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("spark-read-write-formats").getOrCreate()

    input_path = "/opt/spark/pipelines/data/sample.csv"
    output_base = "/opt/spark/pipelines/output"

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    df.write.mode("overwrite").parquet(f"{output_base}/sample_parquet")
    df.write.mode("overwrite").json(f"{output_base}/sample_json")
    df.write.mode("overwrite").option("header", True).csv(f"{output_base}/sample_csv")

    spark.stop()


if __name__ == "__main__":
    main()
