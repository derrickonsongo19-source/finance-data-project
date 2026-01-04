from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Define schema for transactions
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
    StructField("is_fraudulent", BooleanType(), True),
    StructField("description", StringType(), True)
])

# Initialize Spark Session with Kafka package
spark = SparkSession.builder \
    .appName("RealTimeFinanceAnalytics") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🔥 Starting Spark Streaming Consumer...")

# Read from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_transactions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON from Kafka value
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), transaction_schema).alias("data")) \
    .select("data.*")

# 1. Fraud Detection: Flag suspicious transactions
fraud_detection = parsed_df \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("fraud_risk_score", 
        when(col("is_fraudulent"), 100)
        .when((col("amount") < -300) & (col("location") == "INTERNATIONAL"), 85)
        .when((col("amount") < -200) & (hour(to_timestamp(col("timestamp"))) < 6), 70)
        .when((abs(col("amount")) > 500) & (col("category").isin("Food", "Entertainment")), 60)
        .otherwise(10)
    ) \
    .withColumn("fraud_alert", 
        when(col("fraud_risk_score") > 50, "HIGH_RISK")
        .when(col("fraud_risk_score") > 30, "MEDIUM_RISK")
        .otherwise("LOW_RISK")
    )

# 2. Real-time Balance Updates (windowed aggregation)
balance_updates = parsed_df \
    .groupBy(
        window(to_timestamp(col("timestamp")), "5 minutes", "1 minute"),
        col("account_id")
    ) \
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("net_flow"),
        sum(when(col("amount") < 0, col("amount")).otherwise(0)).alias("total_expenses"),
        sum(when(col("amount") > 0, col("amount")).otherwise(0)).alias("total_income")
    ) \
    .withColumn("estimated_balance", col("net_flow") + 10000)  # Starting balance assumption

# 3. Anomaly Detection: Unusual spending patterns
anomaly_detection = parsed_df \
    .filter(col("amount") < 0) \
    .groupBy(
        window(to_timestamp(col("timestamp")), "10 minutes", "2 minutes"),
        col("account_id"),
        col("category")
    ) \
    .agg(
        count("*").alias("tx_count"),
        avg("amount").alias("avg_amount"),
        stddev("amount").alias("amount_stddev")
    ) \
    .withColumn("anomaly_flag",
        when((col("tx_count") > 5) & (abs(col("avg_amount")) > 200), "HIGH_FREQUENCY_LARGE")
        .when(col("amount_stddev") > 150, "VOLATILE_SPENDING")
        .otherwise("NORMAL")
    )

# Console outputs for debugging
fraud_query = fraud_detection \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

balance_query = balance_updates \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .start()

anomaly_query = anomaly_detection \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="20 seconds") \
    .start()

print("✅ Spark Streaming Queries Started")
print("   - Fraud Detection: Every 10 seconds")
print("   - Balance Updates: Every 30 seconds") 
print("   - Anomaly Detection: Every 20 seconds")
print("\n📊 Streaming analytics will appear below...")
print("Press Ctrl+C to stop all queries\n")

# Keep running
fraud_query.awaitTermination()
balance_query.awaitTermination()
anomaly_query.awaitTermination()

# Stop Spark session
spark.stop()
