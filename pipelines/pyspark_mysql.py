"""
MySQL Database Integration with PySpark
Updated for PySpark 3.5+ with modern best practices
"""
import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

# Environment configuration
os.environ['SPARK_USER'] = os.getenv('SPARK_USER', 'wilsonshamim11')

# MySQL configuration - Use environment variables for security
MYSQL_HOST = os.getenv("MYSQL_HOST", "10.10.83.139")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "d1")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "table1")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for MySQL"""
    # Updated MySQL connector to latest version (8.3.0 as of 2026)
    mysql_jar_path = os.getenv(
        'MYSQL_JAR_PATH',
        '/Users/wilsonshamim/Desktop/Streamsets/jars/mysql/mysql-connector-j-8.3.0.jar'
    )
    
    return (SparkSession.builder
            .master("local[*]")
            .appName("PySpark_MySQL_Integration")
            .config("spark.jars", mysql_jar_path)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

def read_mysql_table(spark: SparkSession, table: str) -> DataFrame:
    """Read data from MySQL table"""
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true"
    
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
        
        # Read from MySQL
        df = read_mysql_table(spark, MYSQL_TABLE)
        
        print(f"Table: {MYSQL_TABLE}")
        df.show()
        
        # Select specific column
        df_id = df.select("id")
        print("ID column only:")
        df_id.show()
        
        # Write to CSV
        output_csv = "output/mysql_export.csv"
        df.write.mode("overwrite").option("header", "true").csv(output_csv)
        print(f"Data exported to CSV: {output_csv}")
        
        # Write to Parquet
        output_parquet = "output/mysql_export.parquet"
        df.write.mode("overwrite").parquet(output_parquet)
        print(f"Data exported to Parquet: {output_parquet}")
        
        # Display statistics
        print(f"Total rows: {df.count()}")
        print("\nSchema:")
        df.printSchema()
        
    except Exception as e:
        print(f"Error during MySQL operation: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
