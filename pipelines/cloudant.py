"""
IBM Cloudant Integration with PySpark
Updated for PySpark 3.5+ with modern best practices
"""
import os
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# IBM Cloudant credentials - Use environment variables for security
DATABASE_NAME = os.getenv("CLOUDANT_DATABASE", "example_db")
CLOUDANT_APIKEY = os.getenv("CLOUDANT_APIKEY", "")
CLOUDANT_HOST = os.getenv("CLOUDANT_HOST", "")
CLOUDANT_USERNAME = os.getenv("CLOUDANT_USERNAME", "")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for Cloudant"""
    return (SparkSession.builder
            .appName("CloudantPySpark")
            .config("spark.jars", os.getenv("CLOUDANT_JAR_PATH", 
                   "/home/wsuser/work/spark-sql-cloudant_2.12-2.4.0.jar"))
            .config("cloudant.host", CLOUDANT_HOST)
            .config("cloudant.username", CLOUDANT_USERNAME)
            .config("cloudant.password", CLOUDANT_APIKEY)
            .config("spark.sql.catalogImplementation", "hive")
            .getOrCreate())

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        # Read from Cloudant
        df = spark.read.format("org.apache.bahir.cloudant").load(DATABASE_NAME)
        df.cache()
        df.printSchema()
        df.show(truncate=False)
        
    except Exception as e:
        print(f"Error reading from Cloudant: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
