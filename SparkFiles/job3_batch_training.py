from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, current_date, date_sub
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

# Paths and configurations
DELTA_PATH_CLEAN = "/datalake/transactions_clean"
MODEL_PATH = "/models/lr_model"
CH_URL = "jdbc:clickhouse://localhost:8123/default"
CH_TABLE_BATCH = "transactions_batch"

# Create Spark session with Delta support
spark = SparkSession.builder \
    .appName("Job3-BatchTrainingAndBatchToCH") \
    .getOrCreate()
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# 1) Read batch data from Delta
df = spark.read.format("delta").load(DELTA_PATH_CLEAN)
# Optionally filter by a specific date, e.g., yesterday only:
# df = df.where(to_date("event_time") == date_sub(current_date(), 1))

# 2) Feature engineering
assembler = VectorAssembler(
    inputCols=["amount", "customer_age"],
    outputCol="features"
)
df_ml = assembler.transform(df).na.drop(subset=["label"])

# 3) Train logistic regression model
lr = LogisticRegression(labelCol="label", featuresCol="features")
model = lr.fit(df_ml)

# 4) Save/overwrite model for Job2 (Realtime)
model.write().overwrite().save(MODEL_PATH)
print(f"✅ Model saved to {MODEL_PATH}")

# 5) (Optional) Batch scoring and write to ClickHouse
preds_batch = model.transform(df_ml)
out = preds_batch.select(
    "transaction_id", "customer_id", "amount", "customer_age", "event_time",
    "label",
    preds_batch["prediction"].alias("pred_label"),
    preds_batch["probability"].cast("string").alias("probability_vec")
)

props = {"user": "default", "password": ""}
out.write.mode("overwrite").jdbc(url=CH_URL, table=CH_TABLE_BATCH, properties=props)
print(f"✅ Wrote batch predictions to ClickHouse table `{CH_TABLE_BATCH}`")
