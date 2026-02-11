"""
AWS S3 Write Operations with PySpark
Updated for PySpark 3.5+ with modern best practices
"""
import os
from typing import Optional, List, Tuple
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType

# AWS credentials - Use environment variables for security
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for S3"""
    conf = (SparkConf()
            .setAppName("S3_Write_Integration")
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
            .set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
            .set("spark.hadoop.fs.s3a.multipart.threshold", "52428800")  # 50MB
            .set("spark.sql.adaptive.enabled", "true"))
    
    return SparkSession.builder.config(conf=conf).getOrCreate()

def create_sample_dataframe(spark: SparkSession, 
                           data: Optional[List[Tuple]] = None):
    """Create a sample DataFrame"""
    # Define schema
    schema = StructType([
        StructField('Name', StringType(), True),
        StructField('Age', StringType(), True),
        StructField('Gender', StringType(), True)
    ])
    
    # Use provided data or create empty DataFrame
    if data is None or len(data) == 0:
        # Create empty DataFrame
        return spark.createDataFrame([], schema)
    
    return spark.createDataFrame(data, schema)

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        # Sample data
        sample_data = [
            ("John Doe", "30", "Male"),
            ("Jane Smith", "25", "Female"),
            ("Bob Johnson", "35", "Male")
        ]
        
        # Create DataFrame
        df = create_sample_dataframe(spark, sample_data)
        
        print("Sample DataFrame:")
        df.show(truncate=False)
        
        # S3 output path - Update with your bucket and path
        s3_output_path = os.getenv(
            "S3_OUTPUT_PATH",
            "s3a://streamsets-customer-success-internal/wilsonshamim/output/data.json"
        )
        
        print(f"\nWriting to S3: {s3_output_path}")
        
        # Write to S3 in JSON format
        df.write \
          .format('json') \
          .mode("overwrite") \
          .option("compression", "gzip") \
          .save(s3_output_path)
        
        print("Data successfully written to S3")
        
        # Verify by reading back
        print("\nVerifying data written to S3:")
        df_read = spark.read.json(s3_output_path)
        df_read.show(truncate=False)
        
    except Exception as e:
        print(f"Error writing to S3: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
