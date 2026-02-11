"""
Hive Integration with PySpark
Updated for PySpark 3.5+ with modern best practices
Note: HiveContext is deprecated, use SparkSession with enableHiveSupport()
"""
import os
from typing import Optional
from pyspark.sql import SparkSession

# Hive configuration
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "test1")
HIVE_TABLE = os.getenv("HIVE_TABLE", "table1")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session with Hive support"""
    return (SparkSession.builder
            .master("local[*]")
            .appName("PySpark_Hive_Integration")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.adaptive.enabled", "true")
            .enableHiveSupport()
            .getOrCreate())

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print(f"Reading from Hive table: {HIVE_DATABASE}.{HIVE_TABLE}")
        
        # Method 1: Using SQL
        df = spark.sql(f"SELECT * FROM {HIVE_DATABASE}.{HIVE_TABLE}")
        
        print(f"\nData from {HIVE_DATABASE}.{HIVE_TABLE}:")
        df.show()
        
        # Method 2: Using table() method
        df_table = spark.table(f"{HIVE_DATABASE}.{HIVE_TABLE}")
        
        print("\nUsing table() method:")
        df_table.show()
        
        # Display schema
        print("\nTable schema:")
        df.printSchema()
        
        # Show statistics
        print(f"\nTotal rows: {df.count()}")
        
        # Show available databases
        print("\nAvailable databases:")
        spark.sql("SHOW DATABASES").show()
        
        # Show tables in database
        print(f"\nTables in {HIVE_DATABASE}:")
        spark.sql(f"SHOW TABLES IN {HIVE_DATABASE}").show()
        
    except Exception as e:
        print(f"Error reading from Hive: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
