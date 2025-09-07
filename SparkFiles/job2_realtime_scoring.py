import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegressionModel

# Paths and configurations
DELTA_PATH_CLEAN = "/datalake/transactions_clean"
MODEL_PATH = "/models/lr_model"
CH_URL = "jdbc:clickhouse://localhost:8123/default"
CH_TABLE_RT = "transactions_realtime"

# Create Spark session with Delta support
spark = SparkSession.builder \
    .appName("Job2-RealtimeScoringToCH") \
    .getOrCreate()
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# 1) Read streaming data from Delta (output of Job1)
df_clean_stream = spark.readStream \
    .format("delta") \
    .load(DELTA_PATH_CLEAN)

# 2) Load trained ML model if available
model = None
if os.path.exists(MODEL_PATH):
    try:
        model = LogisticRegressionModel.load(MODEL_PATH)
        print("✅ Loaded pre-trained model for real-time inference.")
    except Exception as e:
        print(f"⚠️ Failed to load model: {e}")
else:
    print("⚠️ No model found at start. Realtime predictions will be skipped.")

# 3) Define foreachBatch function to write micro-batches to ClickHouse
def write_microbatch_to_clickhouse(micro_df, batch_id):
    if model is None:
        print(f"[Batch {batch_id}] No model yet; skipping predictions.")
        return

    preds = model.transform(micro_df)
    out = preds.select(
        "transaction_id", "customer_id", "amount", "customer_age", "event_time",
        col("prediction").alias("pred_label"),
        col("probability").cast("string").alias("probability_vec")
    )

    props = {"user": "default", "password": ""}
    out.write.mode("append").jdbc(url=CH_URL, table=CH_TABLE_RT, properties=props)

# 4) Start the streaming query
query = df_clean_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_microbatch_to_clickhouse) \
    .start()

query.awaitTermination()
