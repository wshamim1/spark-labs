# Spark Lab

Local Spark cluster with submit client and MySQL example using Podman/Docker Compose.

## Services
- Spark master and 2 workers
- Spark submit client
- MySQL 8.0

## Start
1. Start the stack:
   ```bash
   podman compose up -d
   ```

2. Verify Spark UI:
   - http://localhost:8080

## Submit example jobs
All jobs are in `pipelines/` and mounted to `/opt/spark/pipelines`.

- Example aggregation:
  ```bash
  podman compose exec spark-submit /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/pipelines/example_job.py
  ```

- Word count:
  ```bash
  podman compose exec spark-submit /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/pipelines/a_wordcount.py
  ```

- SQL example:
  ```bash
  podman compose exec spark-submit /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/pipelines/a_sql_example.py
  ```

- Join example:
  ```bash
  podman compose exec spark-submit /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/pipelines/a_join_example.py
  ```

- Read CSV and write Parquet/JSON/CSV:
  ```bash
  podman compose exec spark-submit /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/pipelines/a_read_write_formats.py
  ```

- Read CSV and write to MySQL (JDBC driver is auto-downloaded):
  ```bash
  podman compose exec spark-submit /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/pipelines/a_read_to_mysql.py
  ```

## Real-world use cases

### Customer RFM Analysis
Recency-Frequency-Monetary analysis for customer segmentation:
```bash
podman compose exec spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/pipelines/example_customer_rfm.py
```

### ETL Pipeline
Data cleaning, validation, and enrichment:
```bash
podman compose exec spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/pipelines/example_etl_pipeline.py
```

### Sales Analysis
Multi-dimensional sales reporting by category and store:
```bash
podman compose exec spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/pipelines/example_sales_analysis.py
```

### Data Quality Checks
Identify nulls, duplicates, and invalid records:
```bash
podman compose exec spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/pipelines/example_data_quality.py
```

## MySQL details
- Host (from containers): `mysql`
- Port: `3306`
- Database: `sparkdb`
- User: `spark`
- Password: `sparkpass`

## Notes
- JDBC driver is pulled via `spark.jars.packages` and cached under `/tmp/.ivy2`.
- Sample input data: `pipelines/data/sample.csv`
