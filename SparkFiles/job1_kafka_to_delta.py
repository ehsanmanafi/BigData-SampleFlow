from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType

# Paths and configurations
DELTA_PATH_CLEAN = "/datalake/transactions_clean"
CHECKPOINT_CLEAN = "/checkpoints/transactions_clean"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "transactions"

# Create Spark session with Delta support
spark = SparkSession.builder \
    .appName("Job1-KafkaToDelta") \
    .getOrCreate()
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Define schema of Kafka messages
schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("customer_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("customer_age", IntegerType()) \
    .add("event_time", StringType()) \
    .add("label", IntegerType())

# 1) Read streaming data from Kafka
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# 2) Parse JSON and clean data
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

df_clean = df_parsed.filter(col("amount").isNotNull()) \
    .withColumn("event_time", to_timestamp("event_time"))

# 3) Write cleaned data to Delta Lake
query = df_clean.writeStream \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT_CLEAN) \
    .outputMode("append") \
    .start(DELTA_PATH_CLEAN)

query.awaitTermination()
