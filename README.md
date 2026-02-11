# PySpark Tutorials Collection

A comprehensive collection of PySpark 3.5+ examples and tutorials demonstrating integration with various data sources and platforms.

## 📋 Overview

This repository contains updated PySpark code examples following modern best practices for Spark 3.5+. All examples have been updated with:

- ✅ Latest PySpark 3.5+ syntax
- ✅ Environment variable-based configuration for security
- ✅ Type hints and proper documentation
- ✅ Updated dependencies (MySQL Connector 8.3.0, AWS SDK 2.25.x, etc.)
- ✅ Modern Spark configurations (adaptive query execution, etc.)
- ✅ Error handling and logging

## 🗂️ File Structure

### Database Integrations
- **`pyspark_mysql.py`** - MySQL database integration with JDBC
- **`spark_mysql.py`** - Alternative MySQL integration example
- **`spark_orcl.py`** - MySQL/Oracle database integration
- **`mysql_multi_connection.py`** - Multiple MySQL connections example
- **`a_read_to_mysql.py`** - Read and write MySQL with Spark
- **`a_read_to_mysql_cloud.py`** - Cloud MySQL read/write example
- **`kudu_demo.py`** - Apache Kudu integration
- **`pyspark_from_hive.py`** - Hive integration with SparkSession

### Cloud Storage (AWS S3)
- **`read_s3.py`** - Read JSON files from S3
- **`read_s3_csv.py`** - Read CSV files from S3
- **`write_to_s3.py`** - Write data to S3 in JSON format

### Object Storage
- **`connect_to_minio.py`** - MinIO S3-compatible storage with Iceberg support

### Streaming
- **`write_to_kafka.py`** - Apache Kafka streaming integration

### NoSQL
- **`cloudant.py`** - IBM Cloudant NoSQL database integration

### Core Spark Examples
- **`basic_spark_app.py`** - Basic Spark application with UI
- **`sample_spark.py`** - Basic Spark configuration example
- **`rdd_operations_tutorial.py`** - RDD operations tutorial
- **`a_wordcount.py`** - Word count using RDDs

### Spark SQL and DataFrames
- **`a_sql_example.py`** - Spark SQL basics
- **`a_join_example.py`** - Join operations example
- **`a_read_write_formats.py`** - Read/write Parquet/JSON/CSV

### End-to-End Pipelines
- **`example_etl_pipeline.py`** - End-to-end ETL pipeline
- **`example_data_quality.py`** - Data quality checks
- **`example_sales_analysis.py`** - Sales analytics pipeline
- **`example_customer_rfm.py`** - Customer RFM segmentation
- **`example_job.py`** - Job orchestration example

### Observability & Log Processing
- **`example_log_processing.py`** - Parse and aggregate application logs

### AI/ML
- **`example_ml_pipeline.py`** - Spark ML pipeline with feature engineering

### Documentation & Utilities
- **`commands.sh`** - Useful Spark and Hadoop commands
- **`CLOUD_DEPLOYMENT.md`** - Cloud deployment notes

## 🚀 Getting Started

### Run with Docker

```bash
# Build and start the stack
docker compose up -d --build

# Follow logs (optional)
docker compose logs -f

# Stop the stack
docker compose down
```

### Running from the Host (no Docker)

### Prerequisites

```bash
# Install PySpark 3.5+
pip install pyspark==3.5.0

# Install additional dependencies as needed
pip install boto3  # For AWS S3
```

### Environment Variables

Set the following environment variables for secure credential management:

```bash
# MySQL/Database
export MYSQL_HOST="your-host"
export MYSQL_PORT="3306"
export MYSQL_DATABASE="your-db"
export MYSQL_USER="your-user"
export MYSQL_PASSWORD="your-password"

# AWS S3
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"

# MinIO
export MINIO_ACCESS_KEY="your-access-key"
export MINIO_SECRET_KEY="your-secret-key"
export MINIO_ENDPOINT="http://localhost:9000"

# Kafka
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="your-topic"

# Cloudant
export CLOUDANT_APIKEY="your-api-key"
export CLOUDANT_HOST="your-host"
export CLOUDANT_USERNAME="your-username"
```

### Running Examples

```bash
# Basic Spark application
python basic_spark_app.py

# RDD operations tutorial
python rdd_operations_tutorial.py

# SQL + joins
python a_sql_example.py
python a_join_example.py

# Read/write formats
python a_read_write_formats.py

# MySQL integration
python pyspark_mysql.py
python a_read_to_mysql.py
python a_read_to_mysql_cloud.py
python mysql_multi_connection.py

# S3 operations
python read_s3.py
python read_s3_csv.py
python write_to_s3.py

# Kafka streaming
python write_to_kafka.py

# End-to-end pipelines
python example_etl_pipeline.py
python example_data_quality.py
python example_sales_analysis.py
python example_customer_rfm.py

# Log processing
python example_log_processing.py

# AI/ML example
python example_ml_pipeline.py

# Submit to cluster
spark-submit --master yarn --deploy-mode cluster your_script.py
```

## 📦 Updated Dependencies

### MySQL Connector
- **Old**: `mysql-connector-java-8.0.24.jar`
- **New**: `mysql-connector-j-8.3.0.jar`
- **Driver Class**: `com.mysql.cj.jdbc.Driver`

### AWS SDK
- **Old**: `software.amazon.awssdk:bundle:2.17.178`
- **New**: `software.amazon.awssdk:bundle:2.25.11`

### Kafka
- **Package**: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`

### Kudu
- **Package**: `org.apache.kudu:kudu-spark3_2.12:1.17.0`

### Iceberg
- **Package**: `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0`

## 🔧 Key Updates

### Deprecated Features Replaced

1. **HiveContext** → `SparkSession.builder.enableHiveSupport()`
2. **SQLContext** → `SparkSession`
3. **Old MySQL Driver** → `com.mysql.cj.jdbc.Driver`
4. **SparkConf without builder** → Modern builder pattern

### New Features Added

1. **Adaptive Query Execution** - Enabled by default
2. **Type Hints** - Added to all functions
3. **Environment Variables** - For secure credential management
4. **Error Handling** - Comprehensive try-catch blocks
5. **Documentation** - Detailed docstrings

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

## ⚠️ Security Notes

- Never commit credentials to version control
- Use environment variables or secret management systems
- Enable SSL/TLS for database connections
- Use IAM roles when running on cloud platforms

## 📝 License

This is a tutorial collection for educational purposes.

## 🤝 Contributing

Feel free to submit issues and enhancement requests!

---

**Last Updated**: February 2026  
**PySpark Version**: 3.5+  
**Python Version**: 3.8+