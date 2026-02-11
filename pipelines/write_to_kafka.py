"""
Apache Kafka Streaming Integration with PySpark
Updated for PySpark 3.5+ with modern best practices
"""
import os
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType

# Kafka configuration - Use environment variables for security
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "node-1.cluster:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topicabc")

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for Kafka streaming"""
    # Updated Kafka package for Spark 3.5
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    
    return (SparkSession.builder
            .appName("Kafka_Streaming_Integration")
            .config("spark.jars.packages", kafka_package)
            .config("spark.sql.debug.maxToStringFields", "100")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate())

def main() -> None:
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Subscribing to topic: {KAFKA_TOPIC}")
        
        # Read from Kafka stream
        kafka_df = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                    .option("subscribe", KAFKA_TOPIC)
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    .option("includeHeaders", "true")
                    .option("maxOffsetsPerTrigger", "1000")  # Rate limiting
                    .load())
        
        # Display schema
        print("\nKafka Stream Schema:")
        kafka_df.printSchema()
        
        # Parse Kafka message
        parsed_df = kafka_df.selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
            "topic",
            "partition",
            "offset",
            "timestamp",
            "timestampType"
        )
        
        # Write to console for debugging
        query = (parsed_df.writeStream
                 .outputMode("append")
                 .format("console")
                 .option("truncate", "false")
                 .option("numRows", "20")
                 .start())
        
        print("\nStreaming query started. Waiting for data...")
        print("Press Ctrl+C to stop")
        
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping stream...")
    except Exception as e:
        print(f"Error during Kafka streaming: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# Made with Bob
