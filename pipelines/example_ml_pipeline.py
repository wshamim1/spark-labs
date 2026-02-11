from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler


def main() -> None:
    spark = SparkSession.builder.appName("spark-ml-pipeline").getOrCreate()

    data = spark.createDataFrame(
        [
            (25, 45000, "sales", 0),
            (31, 82000, "engineering", 1),
            (29, 61000, "marketing", 0),
            (45, 115000, "engineering", 1),
            (38, 98000, "sales", 1),
            (23, 39000, "support", 0),
            (34, 72000, "marketing", 0),
            (41, 105000, "engineering", 1),
            (28, 52000, "support", 0),
            (37, 88000, "sales", 1),
        ],
        ["age", "salary", "department", "label"],
    )

    indexer = StringIndexer(inputCol="department", outputCol="department_index")
    assembler = VectorAssembler(
        inputCols=["age", "salary", "department_index"], outputCol="features"
    )
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    pipeline = Pipeline(stages=[indexer, assembler, lr])

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train)

    predictions = model.transform(test)
    print("=== Predictions ===")
    predictions.select("age", "salary", "department", "label", "probability", "prediction").show(
        truncate=False
    )

    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)
    print(f"\nAUC: {auc:.4f}")

    feature_importance = model.stages[-1].coefficients
    print(f"\nModel coefficients: {feature_importance}")

    spark.stop()


if __name__ == "__main__":
    main()
