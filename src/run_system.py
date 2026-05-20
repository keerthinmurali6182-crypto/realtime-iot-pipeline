import sys
import os
import yaml
from pyspark.sql import SparkSession

# Ensure Python can find files inside the 'src' directory relative to the workspace root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your custom pipeline class
try:
    from src.pipeline import IoTStreamingPipeline
except ModuleNotFoundError:
    from pipeline import IoTStreamingPipeline


def run_pipeline():
    print("🚀 Initializing Spark Structured Streaming Pipeline...")
    
    # 1. Initialize Spark Session with Windows Local Network Fixes
    spark = SparkSession.builder \
        .appName("Realtime-IoT-Pipeline") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # 2. Safely Load Configuration File
    config_path = "config/iot_config.yaml"
    if not os.path.exists(config_path):
        # Fallback if executing relative to the src directory
        config_path = "../config/iot_config.yaml"
        
    print(f"📦 Loading pipeline configurations from: {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        
    # 3. Instantiate the Pipeline Engine with required parameters
    pipeline = IoTStreamingPipeline(spark, config)
    
    # 4. Start the Streaming Engine
    print("📡 Pipeline started! Awaiting streaming micro-batches from generator...")
    query = pipeline.start_stream()
    
    # 5. Lock execution loop to keep processes alive and active
    if query:
        query.awaitTermination()


if __name__ == "__main__":
    run_pipeline()