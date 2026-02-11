"""
MySQL/Oracle Database Integration with PySpark
Updated for PySpark 3.5+ with modern best practices
Note: Despite filename, this connects to MySQL. Rename if needed.
"""
import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

# Database configuration - Use environment variables for security
DB_HOST = os.getenv("DB_HOST", "west-2.rds.amazonaws.com")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "db1")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_TABLE = os.getenv("DB_TABLE", "test9")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for MySQL"""
    # Updated MySQL connector to latest version (8.3.0 as of 2026)
    mysql_jar_path = os.getenv(
        'MYSQL_JAR_PATH',
        '/Users/wilsonshamim/jars/mysql/mysql-connector-j-8.3.0.jar'
    )
    
    return (SparkSession.builder
            .master("local[*]")
            .appName("PySpark_Database_Integration")
            .config("spark.jars", mysql_jar_path)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

def read_database_table(spark: SparkSession, table: str) -> DataFrame:
    """Read data from MySQL/Oracle database"""
    jdbc_url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?useSSL=true&allowPublicKeyRetrieval=true"
    
    return (spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")  # Updated driver class
            .option("dbtable", table)
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("fetchsize", "1000")
            .load())

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print(f"Connecting to database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        # Read from database
        df = read_database_table(spark, DB_TABLE)
        
        print(f"Table: {DB_TABLE}")
        df.show()
        
        # Select specific column
        df_id = df.select("ID")
        print("ID column only:")
        df_id.show()
        
        # Display schema and statistics
        print("\nSchema:")
        df.printSchema()
        print(f"\nTotal rows: {df.count()}")
        
    except Exception as e:
        print(f"Error during database operation: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
