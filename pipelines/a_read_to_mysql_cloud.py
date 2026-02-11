import os
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("spark-read-to-mysql-cloud").getOrCreate()

    input_path = os.getenv("INPUT_PATH", "/opt/spark/pipelines/data/sample.csv")

    mysql_host = os.getenv("MYSQL_HOST", "mysql")
    mysql_port = os.getenv("MYSQL_PORT", "3306")
    mysql_db = os.getenv("MYSQL_DATABASE", "sparkdb")
    mysql_user = os.getenv("MYSQL_USER", "spark")
    mysql_password = os.getenv("MYSQL_PASSWORD", "sparkpass")
    mysql_table = os.getenv("MYSQL_TABLE", "employees")

    jdbc_url = (
        f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}?"
        "useSSL=false&allowPublicKeyRetrieval=true"
    )

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    df.write.mode("overwrite").format("jdbc").option("url", jdbc_url).option(
        "dbtable", mysql_table
    ).option("user", mysql_user).option(
        "password", mysql_password
    ).option(
        "driver", "com.mysql.cj.jdbc.Driver"
    ).save()

    spark.stop()


if __name__ == "__main__":
    main()
