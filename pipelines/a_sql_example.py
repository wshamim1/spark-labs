from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("spark-sql-example").getOrCreate()

    data = [
        ("alice", "engineering", 120000),
        ("bob", "sales", 90000),
        ("carol", "engineering", 125000),
        ("dave", "sales", 88000),
    ]
    df = spark.createDataFrame(data, ["name", "dept", "salary"])

    df.createOrReplaceTempView("employees")

    result = spark.sql(
        """
        SELECT dept, ROUND(AVG(salary), 2) AS avg_salary
        FROM employees
        GROUP BY dept
        ORDER BY avg_salary DESC
        """
    )

    result.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
