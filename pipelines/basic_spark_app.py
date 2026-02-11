"""
Basic PySpark Application
Updated for PySpark 3.5+ with modern best practices
"""
import os
import time
from pathlib import Path
from pyspark.sql import SparkSession

# Environment configuration
os.environ['SPARK_USER'] = os.getenv('SPARK_USER', 'wilsonshamim')

def create_spark_session() -> SparkSession:
    """Create and configure Spark session"""
    mysql_jar_path = os.getenv("MYSQL_JAR_PATH")

    builder = (SparkSession.builder
            .master("local[*]")  # Use all available cores
            .appName("WordCount")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.ui.port", "4050")
            .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true"))

    if mysql_jar_path:
        jar_path = Path(mysql_jar_path).expanduser()
        if jar_path.exists():
            builder = builder.config("spark.jars", str(jar_path))
        else:
            print(f"Warning: MYSQL_JAR_PATH not found: {jar_path}. Continuing without JDBC driver.")

    return builder.getOrCreate()

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print(f"Spark Session: {spark}")
        print(f"Spark Version: {spark.version}")
        
        # Create sample DataFrame
        df = spark.createDataFrame(
            [(1, "John"), (2, "Jane"), (3, "Bob")], 
            ["id", "name"]
        )
        
        df.show()
        print(f"DataFrame count: {df.count()}")
        
        # Keep Spark UI alive for inspection
        print("Spark UI available at http://localhost:4050")
        print("Waiting 80 seconds for UI inspection...")
        time.sleep(80)
        
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
