# Spark Streaming + Delta Lake + ClickHouse Pipeline

This project contains three Spark jobs that together implement a **Lambda-style data pipeline** with both **streaming (speed layer)** and **batch (batch layer)** processing.

---

## 📂 Jobs Overview

### **Job1 – Kafka → Delta (Streaming Ingestion)**
- Reads raw transaction events from **Kafka** in real time.
- Parses JSON messages, applies basic cleaning (e.g., drop nulls, parse timestamps).
- Writes the cleaned stream to **Delta Lake** (`/datalake/transactions_clean`).
- Uses a **checkpoint** to maintain streaming state.

### **Job2 – Delta → Model → ClickHouse (Realtime Scoring)**
- Reads the **cleaned Delta stream** (output of Job1).
- Loads a **Logistic Regression model** if available.
- Applies the model for **real-time predictions** on each micro-batch.
- Writes prediction results into **ClickHouse** (`transactions_realtime` table).

### **Job3 – Batch Training & Batch Scoring**
- Reads **historical data** from Delta Lake (batch mode).
- Performs **feature engineering** and trains a **Logistic Regression model**.
- Saves/overwrites the model at `/models/lr_model` so Job2 can use it for inference.
- Optionally runs **batch predictions** and writes them to ClickHouse (`transactions_batch` table).

---

## 🔄 Workflow (How They Fit Together)

1. **Job1** ingests Kafka events → cleans → stores in Delta Lake.  
2. **Job2** consumes Delta Lake in streaming mode → applies ML model → stores predictions in ClickHouse (realtime layer).  
3. **Job3** periodically trains/retrains the model with batch data → saves model → also writes batch predictions to ClickHouse (batch layer).  

This design follows the **Lambda Architecture** pattern:  
- **Speed Layer** = Job1 + Job2 (low-latency, real-time scoring).  
- **Batch Layer** = Job3 (accurate model training on historical data).  
- **Serving Layer** = ClickHouse (query-ready predictions).  

---

## ⚙️ Requirements

- **Apache Spark** (with PySpark and Structured Streaming)
- **Delta Lake** (Delta Core libraries available on Spark)
- **Kafka** (as the event source)
- **ClickHouse** (for storing predictions)
- **Python MLlib** (for Logistic Regression training)

---

## 🚀 How to Run

### 1) Start Kafka and produce transaction events
Ensure Kafka broker is running at `localhost:9092` and topic `transactions` exists.

### 2) Run Job1 (Ingestion)
```bash
spark-submit job1_kafka_to_delta.py
```

### 3) Run Job2 (Realtime Scoring)
```bash
spark-submit job2_realtime_scoring.py
```

### 4) Run Job3 (Batch Training)
```bash
spark-submit job3_batch_training.py
```

Job3 should be scheduled (e.g., daily) to retrain the model and refresh predictions.

---

## 📌 Notes
- Job1 and Job2 are **streaming jobs** and run continuously until stopped.  
- Job3 is a **batch job** and finishes after training and writing results.  
- Delta Lake ensures ACID storage in the Data Lake.  
- Checkpoints are used only in streaming jobs (Job1 and Job2).  
