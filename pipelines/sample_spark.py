"""
Basic PySpark Configuration Example
Updated for PySpark 3.5+ with modern best practices
"""
from typing import Optional
from pyspark import SparkContext, SparkConf

def create_spark_context() -> SparkContext:
    """Create and configure Spark context"""
    conf = (SparkConf()
            .setAppName("BasicSparkApp")
            .setMaster("local[*]")
            .set("spark.sql.adaptive.enabled", "true")
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true"))
    
    return SparkContext(conf=conf)

def main() -> None:
    """Main execution function"""
    sc = create_spark_context()
    
    try:
        print(f"SparkContext: {sc}")
        print(f"Spark Version: {sc.version}")
        print(f"Master: {sc.master}")
        print(f"App Name: {sc.appName}")
        
        # Get all configuration
        all_conf = sc.getConf().getAll()
        print("\nSpark Configuration:")
        for key, value in all_conf:
            print(f"  {key}: {value}")
        
        # Example RDD operation
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)
        print(f"\nRDD count: {rdd.count()}")
        print(f"RDD sum: {rdd.sum()}")
        
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        sc.stop()

if __name__ == "__main__":
    main()

# Made with Bob
