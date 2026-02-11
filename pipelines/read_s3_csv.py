"""
AWS S3 CSV Read Operations with PySpark
Updated for PySpark 3.5+ with modern best practices
"""
import os
from typing import Optional
from pyspark.sql import SparkSession
from pyspark import SparkConf

# AWS credentials - Use environment variables for security
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for S3"""
    conf = (SparkConf()
            .setAppName("S3_CSV_Read_Integration")
            .set("spark.executor.extraJavaOptions",
                 "-Dcom.amazonaws.services.s3.enableV4=true")
            .set("spark.driver.extraJavaOptions",
                 "-Dcom.amazonaws.services.s3.enableV4=true")
            .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
            .set("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # Performance optimizations
            .set("spark.hadoop.fs.s3a.connection.maximum", "100")
            .set("spark.hadoop.fs.s3a.threads.max", "50")
            .set("spark.hadoop.fs.s3a.fast.upload", "true")
            .set("spark.sql.adaptive.enabled", "true"))
    
    return SparkSession.builder.config(conf=conf).getOrCreate()

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        # S3 CSV path - Update with your bucket and path
        s3_csv_path = os.getenv(
            "S3_CSV_PATH",
            "s3a://your-bucket/path/to/file.csv"
        )
        
        print(f"Reading CSV from S3: {s3_csv_path}")
        
        # Read CSV from S3 with options
        df = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("mode", "DROPMALFORMED")
              .load(s3_csv_path))
        
        print("\nData from S3 CSV:")
        df.show(truncate=False)
        
        # Display schema
        print("\nSchema:")
        df.printSchema()
        
        # Show statistics
        print(f"\nTotal rows: {df.count()}")
        print(f"Total columns: {len(df.columns)}")
        
        # Show summary statistics
        print("\nSummary statistics:")
        df.describe().show()
        
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
