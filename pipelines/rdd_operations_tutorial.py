"""
Basic PySpark RDD Operations Tutorial
Updated for PySpark 3.5+ with modern best practices

Prerequisites:
- Create test1.txt and test2.txt files with sample content
- Each file should contain comma-separated values
"""
from typing import Optional
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    """Create and configure Spark session"""
    return (SparkSession.builder
            .master("local[*]")
            .appName("BasicRDDOperations")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())

def demonstrate_rdd_operations(sc: SparkContext) -> None:
    """Demonstrate various RDD operations"""
    
    # Read text files
    print("Reading input files...")
    inpfile = sc.textFile('test1.txt')
    test = sc.textFile('test2.txt')
    
    # collect() - Display entire content
    print("\n1. collect() - Display entire content:")
    print(inpfile.collect())
    
    # count() - Display count of lines
    print("\n2. count() - Display count of lines:")
    print(f"Line count: {inpfile.count()}")
    
    # first() - Display first line
    print("\n3. first() - Display first line:")
    print(inpfile.first())
    
    # take(n) - Display first n lines
    print("\n4. take(3) - Display first 3 lines:")
    print(inpfile.take(3))
    
    # takeSample() - Random sample
    print("\n5. takeSample(False, 3, 4) - Random sample:")
    print(inpfile.takeSample(False, 3, 4))
    
    # map() - Transform data
    print("\n6. map() - Split comma-separated lines:")
    x = inpfile.map(lambda line: line.split(","))
    print(x.take(5))
    
    # union() - Merge two RDDs
    print("\n7. union() - Merge content of both files:")
    merged = inpfile.union(test)
    print(merged.collect())
    
    # Additional operations
    print("\n8. Additional operations:")
    
    # Filter operation
    filtered = inpfile.filter(lambda line: len(line) > 0)
    print(f"Non-empty lines: {filtered.count()}")
    
    # Distinct operation
    distinct = merged.distinct()
    print(f"Distinct lines: {distinct.count()}")

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    sc = spark.sparkContext
    
    try:
        print("=" * 60)
        print("PySpark RDD Operations Tutorial")
        print("=" * 60)
        
        demonstrate_rdd_operations(sc)
        
        print("\n" + "=" * 60)
        print("Tutorial completed successfully!")
        print("=" * 60)
        
    except FileNotFoundError as e:
        print(f"\nError: Input files not found - {e}")
        print("Please create test1.txt and test2.txt with sample content")
    except Exception as e:
        print(f"\nError during execution: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

"""
RDD Operations Summary:
-----------------------
1. collect()     : Returns entire content as a list
2. count()       : Returns the count of elements
3. first()       : Returns the first element
4. take(n)       : Returns first n elements
5. takeSample()  : Returns random sample of elements
6. map()         : Transforms each element using a function
7. union()       : Merges two RDDs
8. filter()      : Filters elements based on condition
9. distinct()    : Returns unique elements
"""

# Made with Bob
