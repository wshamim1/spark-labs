"""
MySQL Database Integration with PySpark - Multiple Connections
Updated for PySpark 3.5+ with modern best practices
"""
import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

# MySQL configuration - Use environment variables for security
MYSQL_HOST = os.getenv("MYSQL_HOST", "wilsondb.ctbxv82tncac.us-west-2.rds.amazonaws.com")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "db1")
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "test9")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for MySQL"""
    # Updated MySQL connector to latest version (8.3.0 as of 2026)
    mysql_jar_path = os.getenv(
        'MYSQL_JAR_PATH',
        '/Users/wilsonshamim/jars/mysql/mysql-connector-j-8.3.0.jar'
    )
    
    return (SparkSession.builder
            .master("local[*]")
            .appName("PySpark_MySQL_MultiConnection")
            .config("spark.jars", mysql_jar_path)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

def read_mysql_table(spark: SparkSession, table: str) -> DataFrame:
    """Read data from MySQL table"""
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=true&allowPublicKeyRetrieval=true"
    
    return (spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")  # Updated driver class
            .option("dbtable", table)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .option("fetchsize", "1000")  # Optimize fetch size
            .load())

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print(f"Connecting to MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        
        # First connection
        print("\n=== First Connection ===")
        df1 = read_mysql_table(spark, MYSQL_TABLE)
        
        print(f"Table: {MYSQL_TABLE}")
        df1.show()
        
        # Select specific column
        df1_id = df1.select("ID")
        print("ID column only:")
        df1_id.show()
        
        # Second connection (demonstrating reusability)
        print("\n=== Second Connection ===")
        df2 = read_mysql_table(spark, MYSQL_TABLE)
        
        print(f"Table: {MYSQL_TABLE}")
        df2.show()
        
        # Select specific column
        df2_id = df2.select("ID")
        print("ID column only:")
        df2_id.show()
        
        # Display schema
        print("\n=== Schema Information ===")
        df1.printSchema()
        
        # Show statistics
        print(f"\nTotal rows: {df1.count()}")
        print(f"Total columns: {len(df1.columns)}")
        
        # Compare both DataFrames
        print("\n=== Comparison ===")
        print(f"DataFrame 1 count: {df1.count()}")
        print(f"DataFrame 2 count: {df2.count()}")
        print(f"DataFrames are equal: {df1.collect() == df2.collect()}")
        
    except Exception as e:
        print(f"Error during MySQL operation: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
