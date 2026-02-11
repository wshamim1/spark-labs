"""
MinIO S3-Compatible Storage Integration with PySpark
Updated for PySpark 3.5+ with Iceberg support and modern best practices
"""
import os
from typing import Optional
from pyspark.sql import SparkSession
from pyspark import SparkConf

# Environment configuration
os.environ['SPARK_USER'] = os.getenv('SPARK_USER', 'wilsonshamim')
os.environ['PYSPARK_SUBMIT_ARGS'] = 'pyspark-shell'

# MinIO credentials - Use environment variables for security
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://10.10.83.139:9000")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for MinIO with Iceberg"""
    conf = (SparkConf()
            .setAppName('MinIO_S3_Integration')
            # Updated AWS SDK to latest version (2.25.x as of 2026)
            .set('spark.jars.packages', 
                 'software.amazon.awssdk:bundle:2.25.11,'
                 'software.amazon.awssdk:url-connection-client:2.25.11,'
                 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0')
            # Iceberg SQL Extensions
            .set('spark.sql.extensions', 
                 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .set('spark.sql.catalog.spark_catalog', 
                 'org.apache.iceberg.spark.SparkSessionCatalog')
            .set('spark.sql.catalog.spark_catalog.type', 'hive')
            .set('spark.sql.catalog.local', 
                 'org.apache.iceberg.spark.SparkCatalog')
            .set('spark.sql.catalog.local.type', 'hadoop')
            .set('spark.sql.catalog.local.warehouse', 's3a://warehouse')
            # Optional: Kerberos authentication
            .set("spark.security.authentication", "kerberos"))
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Configure MinIO S3 settings
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Additional performance settings
    hadoop_conf.set("fs.s3a.connection.maximum", "100")
    hadoop_conf.set("fs.s3a.threads.max", "50")
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    
    return spark

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    print("Spark Session Created Successfully")
    
    try:
        # Create sample DataFrame
        df = spark.createDataFrame(
            [(1, "John"), (2, "Jane"), (3, "Bob")], 
            ["id", "name"]
        )
        
        df.show()
        
        # Write to MinIO S3
        output_path = "s3a://warehouse/sample_data"
        df.write.format('json').mode("overwrite").save(output_path)
        print(f"Data written successfully to {output_path}")
        
        # Read back to verify
        df_read = spark.read.format('json').load(output_path)
        print("Data read back from MinIO:")
        df_read.show()
        
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
