from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col


def main() -> None:
    spark = SparkSession.builder.appName("spark-wordcount").getOrCreate()

    lines = spark.createDataFrame(
        [
            ("hello spark spark",),
            ("hello world",),
            ("spark makes big data easy",),
        ],
        ["line"],
    )

    words = lines.select(explode(split(col("line"), " ")).alias("word"))
    counts = words.groupBy("word").count().orderBy(col("count").desc())

    counts.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
