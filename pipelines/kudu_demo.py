"""
Apache Kudu Integration with PySpark
Updated for PySpark 3.5+ with modern best practices
"""
import os
from typing import Optional
from pyspark.sql import SparkSession

# Kudu configuration
KUDU_MASTER = os.getenv("KUDU_MASTER", "localhost:7051")
KUDU_TABLE = os.getenv("KUDU_TABLE", "impala::default.test_kudu")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for Kudu"""
    # Updated Kudu dependencies for Spark 3.5
    kudu_jar_path = os.getenv(
        'KUDU_JAR_PATH',
        'org.apache.kudu:kudu-spark3_2.12:1.17.0'
    )
    
    return (SparkSession.builder
            .master("local[*]")
            .appName("PySpark_Kudu_Integration")
            .config("spark.jars.packages", kudu_jar_path)
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print(f"Connecting to Kudu Master: {KUDU_MASTER}")
        print(f"Reading table: {KUDU_TABLE}")
        
        # Read from Kudu
        df = (spark.read
              .format('kudu')
              .option('kudu.master', KUDU_MASTER)
              .option('kudu.table', KUDU_TABLE)
              .load())
        
        print("Full table:")
        df.show()
        
        # Select specific column
        df_id = df.select("ID")
        print("ID column only:")
        df_id.show()
        
        # Display schema
        print("Table schema:")
        df.printSchema()
        
        # Show statistics
        print(f"Total rows: {df.count()}")
        
    except Exception as e:
        print(f"Error reading from Kudu: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
